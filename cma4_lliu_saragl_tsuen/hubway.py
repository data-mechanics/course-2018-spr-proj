import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hubway(dml.Algorithm):
    contributor = 'cma4_lliu_saragl_tsuen'
    reads = []
    writes = ['cma4_lliu_saragl_tsuen.hubway']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')

        url = 'https://secure.thehubway.com/data/stations.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        stations = r["stations"]
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_lliu_saragl_tsuen.hubway")
        repo.createCollection("cma4_lliu_saragl_tsuen.hubway")
        repo['cma4_lliu_saragl_tsuen.hubway'].insert_many(stations)
        repo['cma4_lliu_saragl_tsuen.hubway'].metadata({'complete':True})
        print(repo['cma4_lliu_saragl_tsuen.hubway'].metadata())

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
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('hub', 'https://secure.thehubway.com/data/')

        this_script = doc.agent('alg:cma4_lliu_saragl_tsuen#hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:hubway', {'prov:label':'Hubway Station Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        hubway = doc.entity('dat:cma4_lliu_saragl_tsuen#hubway', {prov.model.PROV_LABEL:'Hubway Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_stations, endTime)
        doc.wasDerivedFrom(hubway, resource, get_stations, get_stations, get_stations)

        repo.logout()
                  
        return doc

hubway.execute()
doc = hubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof