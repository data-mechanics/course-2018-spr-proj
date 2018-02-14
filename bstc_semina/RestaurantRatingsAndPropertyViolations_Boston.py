import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from tqdm import tqdm

class RestaurantRatingsAndPropertyViolations_Boston(dml.Algorithm):

    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.RestaurantRatingsAndPropertyViolations_Boston']

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
        new_collection_name = 'RestaurantRatingsAndPropertyViolations_Boston'

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        repo.dropCollection('bstc_semina.'+new_collection_name)
        repo.createCollection('bstc_semina.'+new_collection_name)

        collection_yelp = repo.bstc_semina.getBostonYelpRestaurantData
        cursor_yelp = collection_yelp.find({})
        collection_inspections = repo.bstc_semina.getBostonEnforcementData
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

        ## clean property violation data
        # project for StNo, street, suffix, city, state, zip, type of violation, count
        inspection_data = [result for result in tqdm(cursor_inspections, desc='property violation data')]
        inspection_filter = lambda x: { 'StNo':     x['StNo'],
                                        'Street':   x['Street'],
                                        'Suffix':   x['Suffix'],
                                        'City':     x['City'],
                                        'State':    x['State'],
                                        'Zip':      x['Zip'],
                                        'type':     x['Description'],
                                        'count':    1}
        inspection_data = project(inspection_data, inspection_filter)

        # aggregate violations
        def aggregateViolations(R):
            key_properties = ['StNo', 'Street', 'Suffix', 'City', 'State', 'Zip']

            # key by address
            keys = {(r[key_properties[0]],
                    r[key_properties[1]],
                    r[key_properties[2]],
                    r[key_properties[3]],
                    r[key_properties[4]],
                    r[key_properties[5]])
                    for r in R}

            # helper function to see if the key matches the record via address properties
            def compareAddresses(key, r):
                for x in range(len(key_properties)):
                    if (key[x] != r[key_properties[x]]): # compare key (tuple property) with record property (dict entry)
                        return False
                return True # if all pass, return True

            aggregation = []
            for key in tqdm(keys, desc='aggregate violations'):
                results_for_key = [result for result in R if compareAddresses(key, result)] # select
                num_violations = sum([result['count'] for result in results_for_key])

                # create a dictionary containing all types of violations and the counts of occurrences
                violation_types = {}
                for r in results_for_key:
                    # escape $ and . characters to Unicode full-width equivalents
                    r['type'] = r['type'].replace('.','．')
                    r['type'] = r['type'].replace('$','＄')
                    if (r['type'] not in violation_types):
                        violation_types[r['type']] = 1
                    else:
                        violation_types[r['type']] += 1

                aggregation.append({key_properties[0]: key[0],
                                    key_properties[1]: key[1],
                                    key_properties[2]: key[2],
                                    key_properties[3]: key[3],
                                    key_properties[4]: key[4],
                                    key_properties[5]: key[5],
                                    'num_violations': num_violations,
                                    'violations': violation_types})

            return aggregation

        inspection_data = aggregateViolations(inspection_data)
        # by this point, data looks like [
            #{'StNo': '972', 'Street': 'Morton', 'Suffix': 'ST', 'City': 'Dorchester', 'State': 'MA', 'Zip': '02124',
            #'num_violations': 1,
            #'violations': {'Improper storage trash: res': 1}}, ...]

        # print(len(inspection_data), inspection_data[:15])


        ## merge yelp and health inspection data

        # take product of datasets
        ratings_and_inspection_data = product(yelp_data,inspection_data)

        # select ones with common address
        # helper function to see if the addresses in yelp and property violation data matches
        def compareAddresses(t): # takes in tuple of yelp entry and property violation entry
            yelp_r, viol_r = t
            viol_address_properties = ['StNo', 'Street', 'Suffix', 'City', 'State', 'Zip']
            if (yelp_r['location']['address1'] != '{} {} {}'.format(viol_r['StNo'], viol_r['Street'], viol_r['Suffix'])
                or yelp_r['location']['city'] != viol_r['City']
                or yelp_r['location']['zip_code'] != viol_r['Zip']
                or yelp_r['location']['state'] != viol_r['State']): # compare key (tuple property) with record property (dict entry)
                return False
            return True # if all pass, return True
        ratings_and_inspection_data = select(ratings_and_inspection_data, compareAddresses)

        # project to get fields wanted
        project_filter = lambda t: {    'name':t[0]['name'],
                                        'rating':t[0]['rating'],
                                        'review_count':t[0]['review_count'],
                                        'location':t[0]['location'],
                                        'num_property_violations':t[1]['num_violations'],
                                        'property_violations':t[1]['violations']
                                        }
        ratings_and_inspection_data = project(ratings_and_inspection_data, project_filter)
        # by this point, data looks like [
            #{'name': 'Finagle A Bagel',
            #'rating': 3.5,
            #'review_count': 127,
            #'location': {'address1': '535 Boylston St', 'address2': '', 'address3': '', 'city': 'Boston', 'zip_code': '02116', 'country': 'US', 'state': 'MA', 'display_address': ['535 Boylston St', 'Boston, MA 02116']},
            #'num_property_violations': 1,
            #'property_violations': {'Improper storage trash: com': 1}}, ...]

        # print(len(ratings_and_inspection_data), ratings_and_inspection_data[:10])

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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

        return doc

RestaurantRatingsAndPropertyViolations_Boston.execute()
