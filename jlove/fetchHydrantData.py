import urllib.request
import dml
import prov.model
import datetime
import json
import uuid

class fetchHydrantData(dml.Algorithm):
    contributor = 'jlove'
    reads = []
    writes = ['jlove.hydrants']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        
        repo.dropCollection("hydrants")
        repo.createCollection("hydrants")
                
        data = None
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.geojson'
        response = urllib.request.urlopen(url)
        if response.status == 200:
            data = response.read().decode('utf-8')
        if data != None:
            entries = json.loads(data)
            repo['jlove.hydrants'].insert_one(entries)
            repo['jlove.hydrants'].metadata({'complete':True})
            print(repo['jlove.hydrants'].metadata())
        repo.logout()
        
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
        
        
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/dataset/')
        
        this_script = doc.agent('alg:jlove#fetchHydrantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:fire-hydrant', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        fetch_hydrants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(fetch_hydrants, this_script)
        doc.usage(fetch_hydrants, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        hydrants = doc.entity('dat:jlove#hydrants', {prov.model.PROV_LABEL:'Boston fire Hydrants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hydrants, this_script)
        doc.wasGeneratedBy(hydrants, fetch_hydrants, endTime)
        doc.wasDerivedFrom(hydrants, resource, fetch_hydrants, fetch_hydrants, fetch_hydrants)


        repo.logout()
                  
        return doc