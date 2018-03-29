import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieveProperties(dml.Algorithm):
    contributor = 'vinwah'
    reads = []
    writes = ['vinwah.properties']

    @staticmethod
    def execute(trial = False):
        '''
        Retrieves data about properties in Boston from Analyze Boston
        '''
        
        startTime = datetime.datetime.now()

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        # End-point associated with data
        uri = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858&limit=171000'
        
        # response from get request
        response = urllib.request.urlopen(uri).read().decode("utf-8")

        # full json response
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        # remove old collection, and create a new one
        repo.dropCollection("properties")
        repo.createCollection("properties")

        # insert data into collection 
        repo['vinwah.properties'].insert_many(r['result']['records'])

        repo.logout()

        endTime = datetime.datetime.now()

        print('retrieve properties finished at:', endTime)

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
        this_script = doc.agent('alg:retrieveProperties', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:api/3/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label':'Property Assessment FY2017', prov.model.PROV_TYPE:'ont:DataResource'})
        get = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        prop = doc.entity('dat:properties', {prov.model.PROV_LABEL:'Property Assessment FY2017', prov.model.PROV_TYPE:'ont:DataSet'})
        
        # establish relationships 
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'&limit=171000'
                  }
                  )
        doc.wasAttributedTo(prop, this_script)
        doc.wasGeneratedBy(prop, get, endTime)
        doc.wasDerivedFrom(prop, resource, get, get, get)

        repo.logout()
                  
        return doc










