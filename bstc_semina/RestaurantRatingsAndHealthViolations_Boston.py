import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from tqdm import tqdm

class RestaurantRatingsAndHealthViolations_Boston(dml.Algorithm):

    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.RestaurantRatingsAndHealthViolations_Boston']

    @staticmethod
    def execute(trial = False):
        # helper functions
        def project(R, p):
            return [p(t) for t in tqdm(R, desc='project')]
        def product(R, S):
            return [(t,u) for t in tqdm(R, desc='product') for u in S]
        def select(R, s):
            return [t for t in tqdm(R, desc='select') if s(t)]

        startTime = datetime.datetime.now()
        new_collection_name = 'RestaurantRatingsAndHealthViolations_Boston'

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        repo.dropCollection('bstc_semina.'+new_collection_name)
        repo.createCollection('bstc_semina.'+new_collection_name)

        collection_yelp = repo.bstc_semina.getBostonYelpRestaurantData
        cursor_yelp = collection_yelp.find({})
        collection_inspections = repo.bstc_semina.getBostonInspectionData
        cursor_inspections = collection_inspections.find({})

        ## clean yelp data
        # select non-empty yelp data and project for business data only
        yelp_data = []
        [yelp_data.append(result['businesses'][0]) for result in tqdm(cursor_yelp, desc='yelp data')
            if 'businesses' in result # if businesses is a property in result
            and result['businesses']] # check if list contains values

        # project for name, rating, location
        yelp_filter = lambda x: {'name':x['name'],
                                    'rating':x['rating'],
                                    'review_count':x['review_count'],
                                    'location': x['location']}
        yelp_data = project(yelp_data, yelp_filter)
        # print(yelp_data[:10])

        ## clean health inspection data
        # project for name, ViolLevel, count
        inspection_data = [result for result in tqdm(cursor_inspections, desc='health inspection data')]
        inspection_filter = lambda x: {'name':x['businessName'],
                                        'ViolLevel':x['ViolLevel'],
                                        'count':1}
        inspection_data = project(inspection_data, inspection_filter)

        # aggregate violations and calculate average violation level (ave_violation_severity)
        def aggregateViolations(R):
            keys = {r['name'] for r in R}

            aggregation = []
            for key in tqdm(keys, desc='aggregate violations'):
                results_for_key = [result for result in R if result['name'] == key]
                num_violations = sum([result['count'] for result in results_for_key])
                total_severity = sum([len(result['ViolLevel']) for result in results_for_key])
                aggregation.append({'name':key,
                    'num_violations':num_violations,
                    'ave_violation_severity':total_severity/num_violations})

            return aggregation

        inspection_data = aggregateViolations(inspection_data)
        # by this point, data looks like [{'name': '1000 Washington Cafe', 'num_violations': 16, 'ave_violation_severity': 0.875}, ...]

        # print(len(inspection_data))


        ## merge yelp and health inspection data

        # take product of datasets
        ratings_and_inspection_data = product(yelp_data,inspection_data)

        # select ones with common name keys (ignore case)
        name_filter = lambda t: t[0]['name'].lower() == t[1]['name'].lower()
        ratings_and_inspection_data = select(ratings_and_inspection_data, name_filter)

        # project to get fields wanted
        project_filter = lambda t: {'name':t[0]['name'],
                                        'rating':t[0]['rating'],
                                        'review_count':t[0]['review_count'],
                                        'location':t[0]['location'],
                                        'num_violations':t[1]['num_violations'],
                                        'ave_violation_severity':t[1]['ave_violation_severity']
                                        }
        ratings_and_inspection_data = project(ratings_and_inspection_data, project_filter)
        # by this point, data looks like [{'name': '149 Eat Street',
            #'rating': 2.5, 'review_count': 8,
            #'location': {'address1': '149 13th St', 'address2': '', 'address3': '', 'city': 'Charlestown', 'zip_code': '02129', 'country': 'US', 'state': 'MA', 'display_address': ['149 13th St', 'Charlestown, MA 02129']},
            #'num_violations': 83,
            #'ave_violation_severity': 1.4096385542168675}, ...]

        # print(ratings_and_inspection_data)

        ## insert into mongodb
        repo['bstc_semina.'+new_collection_name].insert_many(ratings_and_inspection_data)

        repo.logout()

        endTime = datetime.datetime.now()

        return ({'start':startTime, 'end':endTime})

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bstc_semina') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/bstc_semina') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        

        this_script = doc.agent('alg:bstc_semina#RestaurantRatingsAndPropertyViolations_Boston', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:bstc_semina#dat:getBostonInspectionData', {'prov:label':'Building Code Violations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('dat:bstc_semina#dat:getBostonYelpRestaurantData', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_insp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_yelp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_insp, this_script)
        doc.wasAssociatedWith(get_yelp, this_script)
        doc.usage(get_insp, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=_id,name,location,num_violations,ave_violation_severity'
                  }
                  )
        doc.usage(get_yelp, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=rating,review_count'
                  }
                  )

        isnp = doc.entity('dat:bstc_semina#dat:getBostonEnforcementData', {prov.model.PROV_LABEL:'Boston Inspection Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(isnp, this_script)
        doc.wasGeneratedBy(isnp, get_insp, endTime)
        doc.wasDerivedFrom(isnp, resource1, get_insp, get_insp, get_insp)
        
        yelp = doc.entity('dat:bstc_semina#dat:getBostonYelpRestaurantData', {prov.model.PROV_LABEL:'Boston Yelp Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(yelp, this_script)
        doc.wasGeneratedBy(yelp, get_yelp, endTime)
        doc.wasDerivedFrom(yelp, resource2, get_yelp, get_yelp, get_yelp)

        repo.logout()

        return doc

RestaurantRatingsAndHealthViolations_Boston.execute()
