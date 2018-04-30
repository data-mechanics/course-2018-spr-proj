import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from vincenty import vincenty

class norm_houses(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkHousesAttributes']
    writes = ['aolzh.NewYorkNormHouses']

    @staticmethod
    def execute(trial = False):
        print("norm")
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorkhouses = repo.aolzh.NewYorkHousesAttributes
        newyorkhouses_ = repo.aolzh.NewYorkHousesAttributes
        
        houses = newyorkhouses.find()
        houses_ = newyorkhouses_.find()

        nyc_norm_houses = []

        count_school = []
        count_subway = []
        count_crime = []
        count_hospitals = []
        count_stores = []
        rate = []

        for h in houses_:
            count_school.append(h['school_count'])
            count_crime.append(h['crime_count'])
            count_subway.append(h['subway_count'])
            count_hospitals.append(h['hospital_count'])
            count_stores.append(h['store_count'])
            rate.append(h['rate'])

        houses_.rewind()
        
        #Z-score Normlization

        def avg(x):
            return sum(x)/len(x)
        
        def stddev(x):
            m = avg(x)
            return math.sqrt(sum([(xi-m)**2 for xi in x])/len(x))

        """

        print(avg(count_school))
        print(avg(count_crime))
        print(avg(count_subway))
        print(avg(count_hospitals))
        print(avg(count_stores))
        """

        for h in houses:
            norm_school = (h['school_count'] - avg(count_school))/ stddev(count_school)
            norm_crime = 1- (h['crime_count'] - avg(count_crime))/ stddev(count_crime)
            norm_subway = (h['subway_count'] - avg(count_subway))/ stddev(count_subway)
            norm_hospitals = (h['hospital_count'] - avg(count_hospitals))/ stddev(count_school)
            norm_stores = (h['store_count'] - avg(count_stores))/ stddev(count_stores)
            norm_rate = (h['rate'] -avg(rate)) / stddev(rate)
            nyc_norm_houses.append({'house':h['house'],'rate':h['rate'],'norm_rate': norm_rate, 'norm_crime':norm_crime, 'norm_subway': norm_subway, 'norm_school': norm_school, 'norm_hospitals':norm_hospitals, 'norm_stores': norm_stores})
        	
        repo.dropCollection("NewYorkNormHouses")
        repo.createCollection("NewYorkNormHouses")
        repo["aolzh.NewYorkNormHouses"].insert_many(nyc_norm_houses)
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

        norm_houses_script = doc.agent('alg:aolzh#norm_houses', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_norm_houses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorkhousesattributes_resource = doc.entity('dat:aolzh#NewYorkHousesAttributes', {prov.model.PROV_LABEL:'NewYork Houses Attributes', prov.model.PROV_TYPE:'ont:DataSet'})

        norm_houses_ = doc.entity('dat:aolzh#NewYorkNormHouses', {prov.model.PROV_LABEL:'NewYork Norm Houses', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(get_norm_houses, norm_houses_script)

        doc.usage(get_norm_houses, newyorkhousesattributes_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        doc.wasAttributedTo(norm_houses_, norm_houses_script)
        doc.wasGeneratedBy(norm_houses_, get_norm_houses, endTime)
        doc.wasDerivedFrom(norm_houses_, newyorkhousesattributes_resource,get_norm_houses, get_norm_houses, get_norm_houses)

        repo.logout()
                  
        return doc

norm_houses.execute()
doc = norm_houses.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
