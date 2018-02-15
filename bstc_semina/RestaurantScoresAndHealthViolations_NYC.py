import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from tqdm import tqdm
from time import strptime
import time

class RestaurantScoresAndHealthViolations_NYC(dml.Algorithm):

    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.RestaurantScoresAndHealthViolations_NYC']

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
        new_collection_name = 'RestaurantScoresAndHealthViolations_NYC'

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        repo.dropCollection('bstc_semina.'+new_collection_name)
        repo.createCollection('bstc_semina.'+new_collection_name)

        collection_restaurants = repo.bstc_semina.getNYCRestaurantData
        cursor_restaurants = collection_restaurants.find({})
        collection_inspections = repo.bstc_semina.getNYCInspectionData
        cursor_inspections = collection_inspections.find({})

        ## clean NYC restaurant license data
        # select data with non-empty zip_code, lat and lon
        restaurant_data = [result for result in tqdm(cursor_restaurants, desc='NYC restaurant data')
            if 'zip_code' in result
            and 'longitude_wgs84' in result
            and 'latitude_wgs84' in result]

        # project for name, rating, location
        restaurant_filter = lambda x: {'permit_issuance_date':x['permit_issuance_date'],
                                    'permit_expiration_date':x['permit_expiration_date'],
                                    'permit_type_description':x['permit_type_description'],
                                    'location': {
                                        'building': x['address'],
                                        'street': x['street'],
                                        'borough': x['borough'],
                                        'zip_code': x['zip_code'],
                                        'lon': x['longitude_wgs84'],
                                        'lat': x['latitude_wgs84']
                                        }
                                    }
        restaurant_data = project(restaurant_data, restaurant_filter)
        # print(len(restaurant_data), restaurant_data[0])


        ## clean health inspection data
        # project for name, inspection_date, inspection_type, violation_code, cuisine_description, grade, score, location
        inspection_data = [result for result in tqdm(cursor_inspections, desc='health inspection data')
            if 'grade' in result
            and 'violation_code' in result
            and 'building' in result
            and 'street' in result
            and 'boro' in result
            and 'zipcode' in result
            and 'score' in result]
        inspection_filter = lambda x: {'name':x['dba'],
                                        'inspection_date':x['inspection_date'],
                                        'inspection_type':x['inspection_type'],
                                        'violation_code':x['violation_code'],
                                        'cuisine_description':x['cuisine_description'],
                                        'grade':x['grade'],
                                        'score':x['score'],
                                        'location': {
                                            'building': x['building'],
                                            'street': x['street'],
                                            'borough': x['boro'],
                                            'zip_code': x['zipcode']
                                            }
                                        }
        inspection_data = project(inspection_data, inspection_filter)
        # print(len(inspection_data), inspection_data[0])

        # remove licenses older than 2015
        def removeOldLicenses(restaurant_data):
            new_data = []
            for license_r in tqdm(restaurant_data, desc='removing old licenses older than 2015'):
                try:
                    if(len(license_r['permit_expiration_date']) == 10):
                        date1 = time.strptime(license_r['permit_expiration_date'], "%Y-%m-%d")
                    else:
                        date1 = time.strptime(license_r['permit_expiration_date'], "%m/%d/%Y %H:%M:%S")
                    if (date1 > time.strptime('2016-01-01', "%Y-%m-%d")):
                        new_data.append(license_r)
                except ValueError: # some data is malformatted
                    pass
                    # print(license_r['permit_expiration_date'])
            return new_data
        restaurant_data = removeOldLicenses(restaurant_data)
        # print(len(restaurant_data), restaurant_data[0])

        # select violations that correspond to license data
        def combineViolationsAndLicenses(inspection_data, restaurant_data):
            addr_props = ['building', 'street', 'borough', 'zip_code']
            restaurants_and_inspection_data = []

            # helper function to see if the records match via address properties
            def compareAddresses(insp_r, license_r):
                license_r_dict = {addr_props[0]: license_r['location'][addr_props[0]],
                                    addr_props[1]: license_r['location'][addr_props[1]],
                                    addr_props[2]: license_r['location'][addr_props[2]],
                                    addr_props[3]: license_r['location'][addr_props[3]]}
                return license_r_dict == insp_r['location'] # if location values are the same

            def withinLicenseDates(insp_r, license_r):
                insp_date = time.strptime(insp_r['inspection_date'], "%Y-%m-%dT%H:%M:%S")
                if(len(license_r['permit_expiration_date']) == 10):
                    license_exp_date = time.strptime(license_r['permit_expiration_date'], "%Y-%m-%d")
                    license_iss_date = time.strptime(license_r['permit_issuance_date'], "%Y-%m-%d")
                else:
                    license_exp_date = time.strptime(license_r['permit_expiration_date'], "%m/%d/%Y %H:%M:%S")
                    license_iss_date = time.strptime(license_r['permit_issuance_date'], "%m/%d/%Y %H:%M:%S")
                return insp_date < license_exp_date and insp_date > license_iss_date

            for insp_r in tqdm(inspection_data, desc='left joining violations with license data'):
                for license_r in restaurant_data:
                    if (compareAddresses(insp_r, license_r)         # if addresses match
                        and withinLicenseDates(insp_r, license_r)): # and insp is within license dates
                        restaurants_and_inspection_data.append({
                            'name':insp_r['name'],
                            'permit_issuance_date':license_r['permit_issuance_date'],
                            'permit_expiration_date':license_r['permit_expiration_date'],
                            'permit_type_description':license_r['permit_type_description'],
                            'inspection_date':insp_r['inspection_date'],
                            'inspection_type':insp_r['inspection_type'],
                            'violation_code':insp_r['violation_code'],
                            'cuisine_description':insp_r['cuisine_description'],
                            'grade':insp_r['grade'],
                            'score':insp_r['score'],
                            'location':license_r['location']
                            })
                        break

            return restaurants_and_inspection_data

        restaurants_and_inspection_data = combineViolationsAndLicenses(inspection_data, restaurant_data)
        # print(len(restaurants_and_inspection_data), restaurants_and_inspection_data)

        # aggregate violations and calculate average score (ave_score)
        def aggregateViolations(R):
            key_properties = ['building', 'street', 'borough', 'zip_code', 'lon', 'lat', 'permit_expiration_date']

            # key by address
            keys = {(r['location'][key_properties[0]],
                    r['location'][key_properties[1]],
                    r['location'][key_properties[2]],
                    r['location'][key_properties[3]],
                    r['location'][key_properties[4]],
                    r['location'][key_properties[5]],
                    r[key_properties[6]])
                    for r in R}

            # helper function to see if the key matches the record via address properties
            def compareAddresses(key, r):
                # build location dictionary
                key_loc_dict = {key_properties[0]: key[0],
                            key_properties[1]: key[1],
                            key_properties[2]: key[2],
                            key_properties[3]: key[3],
                            key_properties[4]: key[4],
                            key_properties[5]: key[5]}
                # if location values are the same and permit matches
                return key_loc_dict == r['location'] and key[6] == r['permit_expiration_date']

            aggregation = []
            for key in tqdm(keys, desc='aggregate violations'):
                results_for_key = [result for result in R if compareAddresses(key, result)]
                num_violations = len(results_for_key)
                print(results_for_key)
                total_score = sum([int(result['score']) for result in results_for_key])

                aggregation.append({'name':results_for_key[0]['name'],
                    'permit_issuance_date':results_for_key[0]['permit_issuance_date'],
                    'permit_expiration_date':results_for_key[0]['permit_expiration_date'],
                    'permit_type_description':results_for_key[0]['permit_type_description'],
                    'inspection_date':results_for_key[0]['inspection_date'],
                    'inspection_type':results_for_key[0]['inspection_type'],
                    'violation_code':results_for_key[0]['violation_code'],
                    'cuisine_description':results_for_key[0]['cuisine_description'],
                    'grade':results_for_key[0]['grade'],
                    'num_violations':num_violations,
                    'ave_score':total_score/num_violations,
                    'location':results_for_key[0]['location']
                    })

            return aggregation

        restaurants_and_inspection_data = aggregateViolations(restaurants_and_inspection_data)
        # by this point, data looks like [{'name': '1000 Washington Cafe', 'num_violations': 16, 'ave_violation_severity': 0.875}, ...]

        print(len(restaurants_and_inspection_data), restaurants_and_inspection_data[0])

        ## insert into mongodb
        repo['bstc_semina.'+new_collection_name].insert_many(restaurants_and_inspection_data)

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

RestaurantScoresAndHealthViolations_NYC.execute()
