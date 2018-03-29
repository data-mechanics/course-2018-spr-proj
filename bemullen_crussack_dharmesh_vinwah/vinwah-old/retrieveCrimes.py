import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieveCrimes(dml.Algorithm):
    contributor = 'vinwah'
    reads = []
    writes = ['vinwah.crimes']

    @staticmethod
    def execute(trial = False):
        '''
        Retrieves Boston crime data from Analyze Boston
        '''
        
        startTime = datetime.datetime.now()

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        # End-point associated with data in json format
        uri = 'https://data.boston.gov/api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=10000'
        
        # response from get request
        response = urllib.request.urlopen(uri).read().decode("utf-8")

        # full json response
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        # remove old collection, and create a new one
        repo.dropCollection("crimes")
        repo.createCollection("crimes")

        # insert data into collection 
        repo['vinwah.crimes'].insert_many(r['result']['records'])

        repo.logout()

        endTime = datetime.datetime.now()

        print('retrieve crimes finished at:', endTime)

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
        repo.authenticate('vinwah', 'vinwah')

        # set up namespace
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/vinwah#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/vinwah#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/')

        # Set up agent, entities, activity
        this_script = doc.agent('alg:retrieveCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource'})
        get = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        crimes = doc.entity('dat:crimes', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        
        # establish relationships 
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'&limit=10000'
                  }
                  )
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get, endTime)
        doc.wasDerivedFrom(crimes, resource, get, get, get)

        repo.logout()
                  
        return doc










