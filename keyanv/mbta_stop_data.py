import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time

class mbta_stop_data(dml.Algorithm):
    contributor = 'keyanv'
    reads = []
    writes = ['keyanv.mbta_stops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        url = 'https://api-v3.mbta.com/stops'
        response = urllib.request.urlopen(url).read().decode("utf-8")   
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta_stops")
        repo.createCollection("mbta_stops")
        repo['keyanv.mbta_stops'].insert_many(r['data'])
        repo['keyanv.mbta_stops'].metadata({'complete':True})
        print(repo['keyanv.mbta_stops'].metadata())

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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mbt', 'https://api-v3.mbta.com/')

        this_script = doc.agent('alg:keyanv#mbta_stop_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbt:stops', {'prov:label':'MBTA Stop Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':''})
        get_mbta_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta_stops, this_script)
        doc.usage(get_mbta_stops, resource, startTime)
        mbta_stops = doc.entity('dat:keyanv#mbta_stops', {prov.model.PROV_LABEL:'MBTA Stop Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
        doc.wasDerivedFrom(mbta_stops, resource, get_mbta_stops, get_mbta_stops, get_mbta_stops)

        repo.logout()
                  
        return doc

mbta_stop_data.execute()
doc = mbta_stop_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
