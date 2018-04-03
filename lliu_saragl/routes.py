import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class routes(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = []
    writes = ['lliu_saragl.routes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)
        repo.dropPermanent("routes")
        repo.createPermanent("routes")
        
        repo['lliu_saragl.routes'].insert_many(r['features'])
        repo['lliu_saragl.routes'].metadata({'complete':True})
        print(repo['lliu_saragl.routes'].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenacne document describing everything happening in this script.
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bos', 'http://bostonopendata.boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:lliu_saragl#routes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bos:4f3e4492e36f4907bcd307b131afe4a5_0', {'prov:label': 'Snow Emergency Routes', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        get_routes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_routes, this_script)
        
        doc.usage(get_routes, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        routes = doc.entity('dat:lliu_saragl#routes',{prov.model.PROV_LABEL:'Snow Emergency Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(routes, this_script)
        doc.wasGeneratedBy(routes, get_routes, endTime)
        doc.wasDerivedFrom(routes, get_routes, get_routes, get_routes)

        repo.logout()

        return doc

#routes.execute()
#doc = routes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
