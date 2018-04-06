import urllib.request
import dml
import prov.model
import datetime
import json
import uuid
import requests

class fetchNeighborhoodBorders(dml.Algorithm):
    contributor = 'jlove'
    reads = []
    writes = ['jlove.neighborhoods']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        
        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
                
        data = None
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
        if data != None:
            repo['jlove.neighborhoods'].insert_one(data)
            repo['jlove.neighborhoods'].metadata({'complete':True})
            print(repo['jlove.neighborhoods'].metadata())
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
        
        
        this_script = doc.agent('alg:jlove#fetchNeighborhoodBorders', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        fetch_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(fetch_neighborhoods, this_script)
        doc.usage(fetch_neighborhoods, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        neighborhoods = doc.entity('dat:jlove#neighborhoods', {prov.model.PROV_LABEL:'Boston Neighborhood Boundries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, fetch_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, resource, fetch_neighborhoods, fetch_neighborhoods, fetch_neighborhoods)


        repo.logout()
                  
        return doc
