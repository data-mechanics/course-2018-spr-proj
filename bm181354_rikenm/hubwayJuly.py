import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class hubwayJuly(dml.Algorithm):
    contributor = 'bm181354_rikenm'
    reads = []
    writes = ['bm181354_rikenm.hubwayJuly']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
        
        # Dataset01
        url = 'http://datamechanics.io/data/bm181354_rikenm/Hubway_Stations_as_of_July_2017.csv'
 
        hubway_df = pd.read_csv(url)
        # creating df that only contains city, total number of service, EMS_INDEX
   
        r = json.loads(hubway_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=False, indent=2)

        # clear
        repo.dropPermanent('hubwayJuly')
        repo.createPermanent('hubwayJuly')
        repo['bm181354_rikenm.hubwayJuly'].insert_many(r)


        # logout
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
        
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
        doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/?prefix=bm181354_rikenm/')
        
        this_script = doc.agent('alg:bm181354_rikenm#hubwayJuly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # change this
        resource = doc.entity('bdp:Emergency_Medical_Service_EMS_Stations', {'prov:label':'dataset of medical service in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_hubwayJuly = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_hubwayJuly, this_script)
        
        #change this
        doc.usage(get_hubwayJuly, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
                  
        hubwayJuly = doc.entity('dat:bm181354_rikenm#hubwayJuly', {prov.model.PROV_LABEL:'hubwayJuly', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(hubwayJuly, this_script)
        doc.wasGeneratedBy(hubwayJuly, get_hubwayJuly, endTime)
        doc.wasDerivedFrom(hubwayJuly, resource, get_hubwayJuly, get_hubwayJuly, get_hubwayJuly)
        
                  
        repo.logout()
        return doc

## eof


