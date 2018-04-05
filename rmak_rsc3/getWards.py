import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getWards(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getWards'] 

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/398ee443f4ac49e9a0b7facf80afc20f_8.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)['features']
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getWards") 
        repo.createCollection("getWards")
        
        coordinates = {}
        for x in r:
            
            coordinates[x['properties']['WARD']] = x['geometry']['coordinates']
    
#        print((coordinates[1][0][0], coordinates[1][0][1]))  
        print(coordinates[1][0][0][1])
        cordFinal = [{'wardID': k, 'coordinates': coordinates[k][0]} for k in coordinates]
        print(cordFinal)
        repo['rmak_rsc3.getWards'].insert_many(cordFinal)
        
#        repo['rmak_rsc3.wards'].insert_many(r)
    
        repo['rmak_rsc3.getWards'].metadata({'complete':True})
#        print(repo['rmak_rsc3.wards'].metadata())
        
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

        this_script = doc.agent('alg:rmak_rsc3#getWards', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('bdp:398ee443f4ac49e9a0b7facf80afc20f_8', {'prov:label':'Wards', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        getWards = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getWards, this_script)

        doc.usage(getWards, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        Wards = doc.entity('dat:rmak_rsc3#wards', {prov.model.PROV_LABEL:'wards', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Wards, this_script)
        doc.wasGeneratedBy(Wards, getWards, endTime)
        doc.wasDerivedFrom(Wards, resource, getWards, getWards, getWards)
        
        

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
