import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieveUniversities(dml.Algorithm):
    contributor = 'vinwah'
    reads = []
    writes = ['vinwah.universities']

    @staticmethod
    def execute(trial = False):
        '''
        Retrieve geographical location data of universities and coleges 
        from Analyze Boston (universities will be used as reference to 
        both – no distinction is made).
        '''


        startTime = datetime.datetime.now()

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        # End-point associated with data in geojson format
        uri = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        
        # response from get request
        response = urllib.request.urlopen(uri).read().decode("utf-8")

        # response as json
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        # remove old collection, and create a new one
        repo.dropCollection("universities")
        repo.createCollection("universities")

        # insert data into collection 
        repo['vinwah.universities'].insert_many(r['features'])

        repo.logout()

        endTime = datetime.datetime.now()
        print('retrieve universities finished at:', endTime)
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
        doc.add_namespace('bdp_1', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        # Set up agent, entities, activity
        this_script = doc.agent('alg:retrieveUniversities', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp_1:cbf14bb032ef4bd38e20429f71acb61a_2', {'prov:label':'Boston Universities and Colleges', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        universities = doc.entity('dat:universities', {prov.model.PROV_LABEL:'Boston Universities and Colleges', prov.model.PROV_TYPE:'ont:DataSet'})
        
        # establish relationships 
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        doc.wasAttributedTo(universities, this_script)
        doc.wasGeneratedBy(universities, get, endTime)
        doc.wasDerivedFrom(universities, resource, get, get, get)

        repo.logout()
                  
        return doc
