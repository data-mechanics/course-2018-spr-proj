import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
from z3 import *

''' helper function to calculate the distance between 2 geolocations '''
def geodistance(la1, lon1, la2, lon2):
        la1 = radians(la1)
        lon1 = radians(lon1)
        la2 = radians(la2)
        lon2 = radians(lon2)

        dlon = lon1 - lon2

        EARTH_R = 6372.8

        y = sqrt(
            (cos(la2) * sin(dlon)) ** 2
            + (cos(la1) * sin(la2) - sin(la1) * cos(la2) * cos(dlon)) ** 2
            )
        x = sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(dlon)
        c = atan2(y, x)
        return EARTH_R * c


class findCrimeStats(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = ['janellc_rstiffel_yash.crimesData']
    writes = ['janellc_rstiffel_yash.crimeStats']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get the crime data (from yash's project #1)
        #crimesData = repo.janellc_rstiffel_yash.crimesData.find()
        x = Real('x')
        y = Real('y')
        s = Solver()
        s.add(x + y > 5, x > 1, y > 1)
        print(s.check())
        print(s.model())


        #repo.dropCollection("crimeStats")
        #repo.createCollection("crimeStats")

        #repo['janellc_rstiffel_yash.crimeStats'].metadata({'complete':True})
        #print(repo['janellc_rstiffel_yash.crimeStats'].metadata())

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
        repo.authenticate('yash', 'yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ybavishi#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ybavishi#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cri', 'https://data.boston.gov/')

        this_script = doc.agent('alg:getCrimeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cri:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'api/3/action/datastore_search?offset=$&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'
                  }
                  )
        
        prices = doc.entity('dat:crimesData', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

findCrimeStats.execute()
#doc = getCrimeData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
## eof