import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getNeighborhoods(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getNeighborhoods'] 

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)['features']
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getNeighborhoods") 
        repo.createCollection("getNeighborhoods")
        repo['rmak_rsc3.getNeighborhoods'].insert_many(r)
    
        repo['rmak_rsc3.getNeighborhoods'].metadata({'complete':True})
        print(repo['rmak_rsc3.getNeighborhoods'].metadata())
        

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
        repo.authenticate('rmak_rsc3', 'rmak_rsc3')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:rmak_rsc3#getNeighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('bdp:3525b0ee6e6b427f9aab5d0a1d0a1a28_0', {'prov:label':'Neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        getNeighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getNeighborhoods, this_script)

        doc.usage(getNeighborhoods, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        Neighb = doc.entity('dat:rmak_rsc3#neighborhoods', {prov.model.PROV_LABEL:'neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Neighb, this_script)
        doc.wasGeneratedBy(Neighb, getNeighborhoods, endTime)
        doc.wasDerivedFrom(Neighb, resource, getNeighborhoods, getNeighborhoods, getNeighborhoods)
        
        

        repo.logout()
                  
        return doc
    '''
print('get_fireHydrant.execute()')
getUniversities.execute()
print('doc = get_fireHydrant.provenance()')
doc = getUniversities.provenance()
print('doc.get_provn()')
print(doc.get_provn())
print('json.dumps(json.loads(doc.serialize()), indent=4')
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
