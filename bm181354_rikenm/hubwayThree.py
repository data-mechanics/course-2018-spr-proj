import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class hubwayThree(dml.Algorithm):
    contributor = 'bm181354_rikenm'
    reads = []
    writes = ['bm181354_rikenm.hubwayThree']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
        
        # Dataset01
        url = 'http://datamechanics.io/data/bm181354_rikenm/201704-hubway-tripdata.csv'
 
        hubway_df = pd.read_csv(url)
        # creating df that only contains city, total number of service, EMS_INDEX
   
        r = json.loads(hubway_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=False, indent=2)

        # clear
        repo.dropPermanent('hubwayThree')
        repo.createPermanent('hubwayThree')
        repo['bm181354_rikenm.hubwayThree'].insert_many(r)


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
        
        this_script = doc.agent('alg:bm181354_rikenm#hubwayThree', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # change this
        resource = doc.entity('bdp:Emergency_Medical_Service_EMS_Stations', {'prov:label':'dataset of medical service in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_hubwayThree = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_hubwayThree, this_script)
        
        #change this
        doc.usage(get_hubwayThree, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
                  
        hubwayThree = doc.entity('dat:bm181354_rikenm#hubwayThree', {prov.model.PROV_LABEL:'hubwayThree', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(hubwayThree, this_script)
        doc.wasGeneratedBy(hubwayThree, get_hubwayThree, endTime)
        doc.wasDerivedFrom(hubwayThree, resource, get_hubwayThree, get_hubwayThree, get_hubwayThree)
        
                  
        repo.logout()
        return doc

## eof


