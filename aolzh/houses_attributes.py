import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from vincenty import vincenty

class houses_attributes(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkSchool','aolzh.NewYorkCrime','aolzh.NewYorkSubway', 'aolzh.NewYorkHospitals', 'aolzh.NewYorkStores','aolzh.NewYorkHouses']
    writes = ['aolzh.NewYorkHousesAttributes']

    @staticmethod
    def execute(trial = False):
        print("houses_attributes")
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorksubway = repo.aolzh.NewYorkSubway
        newyorkcrime = repo.aolzh.NewYorkCrime
        newyorkschool = repo.aolzh.NewYorkSchool
        newyorkhospitals = repo.aolzh.NewYorkHospitals
        newyorkstroes = repo.aolzh.NewYorkStores
        newyorkhouses = repo.aolzh.NewYorkHouses

        subway = newyorksubway.find()
        crime = newyorkcrime.find()
        school = newyorkschool.find()
        hospitals = newyorkhospitals.find()
        stores = newyorkstroes.find()
        houses = newyorkhouses.find()
        houses_ = newyorkhouses.find()

        nyc_houses_attributes = []

        total = 0
        count = 0
        for h in houses_:
            total += h['Price']
            count += 1

        houses_mean = int(total/ count)
        print(houses_mean)

        houses_.rewind()

        for h in houses:
            location_h = (float(h['latitude']),float(h['longitude']))
            count_school = 0
            count_subway = 0
            count_crime = 0
            count_hospitals = 0
            count_stores = 0

            if abs(h['Price'] - houses_mean) <= 300:
                rate = 3
            elif abs(h['Price'] - houses_mean) <= 600:
                if houses_mean > h['Price']:
                    rate = 2
                else:
                    rate = 4
            else:
                if houses_mean > h['Price']:
                    rate = 1
                else:
                    rate = 5
            for sub in subway:
                location_s = (float(sub['the_geom']['coordinates'][1]), float(sub['the_geom']['coordinates'][0]))
                dis = vincenty(location_s,location_h,miles=True)
                if dis < 0.5:
                    count_subway += 1
            for c in crime:
                if 'latitude' and 'longitude' in c:
                    location_c = (float(c['latitude']),float(c['longitude']))
                    dis = vincenty(location_s,location_h,miles=True)
                    if dis < 1:
                        count_crime += 1
            for sc in school:
                location_sc = (float(sc['the_geom']['coordinates'][1]), float(sc['the_geom']['coordinates'][0]))
                dis = vincenty(location_sc,location_h,miles=True)
                if dis < 2:
                    count_school += 1
            for ho in hospitals:
                location_ho = (float(ho['location_1']['latitude']),float(ho['location_1']['longitude']))
                dis = vincenty(location_ho,location_h,miles=True)
                if dis < 2:
                    count_hospitals += 1
            for st in stores:
                temp = st['Location'].split(',')
                location_st = (float(temp[0]),float(temp[1]))
                dis = vincenty(location_st,location_h,miles=True)
                if dis < 0.5:
                    count_stores += 1
            nyc_houses_attributes.append({'house':h['address'],'rate':rate,'school_count':count_school,'crime_count':count_crime,'subway_count':count_subway,'hospital_count':count_hospitals,'store_count':count_stores})
            subway.rewind()
            crime.rewind()
            school.rewind()
            hospitals.rewind()
            stores.rewind()
        	
        repo.dropCollection("NewYorkHousesAttributes")
        repo.createCollection("NewYorkHousesAttributes")
        repo["aolzh.NewYorkHousesAttributes"].insert_many(nyc_houses_attributes)
        print("Finished")

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        houses_attributes_script = doc.agent('alg:aolzh#houses_attributes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_houses_attributes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorksubway_resource = doc.entity('dat:aolzh#NewYorkSubway', {prov.model.PROV_LABEL:'NewYork Subways', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        newyorkcrime_resource = doc.entity('dat:aolzh#NewYorkCrime', {prov.model.PROV_LABEL:'NewYork Crime', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        newyorkschool_resource = doc.entity('dat:aolzh#NewYorkSchool', {prov.model.PROV_LABEL:'NewYork School', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        newyorkhospitals_resource = doc.entity('dat:aolzh#NewYorkHospitals', {prov.model.PROV_LABEL:'NewYork Hospitals', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        newyorkstores_resource = doc.entity('dat:aolzh#NewYorkStores', {prov.model.PROV_LABEL:'NewYork Stores', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        newyorkhouses_resource = doc.entity('dat:aolzh#NewYorkHouses', {prov.model.PROV_LABEL:'NewYork Houses', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        houses_attributes_ = doc.entity('dat:aolzh#NewYorkHousesAttributes', {prov.model.PROV_LABEL:'NewYork Houses Attributes', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(get_houses_attributes, houses_attributes_script)
        doc.usage(get_houses_attributes, newyorksubway_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_houses_attributes, newyorkcrime_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_houses_attributes, newyorkschool_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_houses_attributes, newyorkstores_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_houses_attributes, newyorkhospitals_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_houses_attributes, newyorkhouses_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        
        doc.wasAttributedTo(houses_attributes_, houses_attributes_script)
        doc.wasGeneratedBy(houses_attributes_, get_houses_attributes, endTime)
        doc.wasDerivedFrom(houses_attributes_, newyorkcrime_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorksubway_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorkschool_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorkhospitals_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorkstores_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorkstores_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
        doc.wasDerivedFrom(houses_attributes_, newyorkhouses_resource,get_houses_attributes, get_houses_attributes, get_houses_attributes)
       

        repo.logout()
                  
        return doc

houses_attributes.execute()
doc = houses_attributes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
