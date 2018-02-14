import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta(dml.Algorithm):
    contributor = 'cma4_tsuen'
    reads = []
    writes = ['cma4_tsuen.mbta']

    def convertTxtToJSON():
        with open('./../data/MBTA_Stops.txt', 'r') as myfile:
            data=myfile.read().replace('\n', '')

        

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_tsuen.mbta")
        repo.createCollection("cma4_tsuen.mbta")
        repo['cma4_tsuen.mbta'].insert_many(r)
        repo['cma4_tsuen.mbta'].metadata({'complete':True})
        print(repo['cma4_tsuen.mbta'].metadata())

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
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

        this_script = doc.agent('alg:cma4_tsuen#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbta:stops', {'prov:label':'MBTA Stops Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        mbta = doc.entity('dat:cma4_tsuen#mbta', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_stops, endTime)
        doc.wasDerivedFrom(mbta, resource, get_stops, get_stops, get_stops)

        repo.logout()
                  
        return doc

mbta.convertTxtToJSON()
#mbta.execute()
#doc = mbta.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
