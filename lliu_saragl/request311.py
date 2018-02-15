import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class request311(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = []
    writes = ['lliu_saragl.requests'] #example has two in here not one

    @staticmethod
    def execute(trial = False):
        '''Retrieve data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=2968e2c0-d479-49ba-a884-4ef523ada3c0&q=snow'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)
        repo.dropPermanent("requests")
        repo.createPermanent("requests")
        
        repo['lliu_saragl.requests'].insert_many(r['result']['records'])
        repo['lliu_saragl.requests'].metadata({'complete':True})
        print(repo['lliu_saragl.requests'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening in this script.
        Each run of the script will generate a new document describing that invocation event.
        '''

        # Set up the database connection 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        #additional resource
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:lliu_saragl#requests', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:2968e2c0-d479-49ba-a884-4ef523ada3c0', {'prov:label': '311 Requests regarding Snow', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_311, this_script)
        
        doc.usage(get_311, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        requests = doc.entity('dat:lliu_saragl#request311',{prov.model.PROV_LABEL:'311 Requests regarding Snow', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(requests, this_script)
        doc.wasGeneratedBy(requests, get_311, endTime)
        doc.wasDerivedFrom(requests, get_311, get_311, get_311)

        #repo.record(doc.serialize())
        repo.logout()

        return doc

request311.execute()
doc = request311.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
