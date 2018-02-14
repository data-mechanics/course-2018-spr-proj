import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class ems(dml.Algorithm):
    contributor = 'biken_riken'
    reads = []
    writes = ['biken_riken.ems']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('biken_riken', 'biken_riken')
        
        # Dataset01
        url = 'http://datamechanics.io/data/bm181354_rikenm/Emergency_Medical_Service_EMS_Stations.csv'
 
        medical_df = pd.read_csv(url)
        
        #mass_list = medical_df['STATE']
        #print(mass_list)
        mass_df = medical_df[medical_df.STATE == 'MA']
        
        # all city from Massachusetts
        mass_city = mass_df['CITY'] # list

        # (city, overall number of emergency service)
        mass_df = mass_df[mass_df['CITY'].apply(lambda l:l.upper()).isin(mass_city)].CITY.value_counts()
        

        # average
        mass_em_index = (mass_df /  mass_df.mean())

        # converting into 1 - 5 scale
        highest = mass_em_index[0]
        mass_em_index = 1 + (mass_df /  mass_df.mean())/highest * 4

        # creating df that only contains city, total number of service, EMS_INDEX
        n = pd.DataFrame(
                 {
                 'CITY_SERVICE':mass_df,
                 'EMS_INDEX': mass_em_index
                 })
        
        r = json.loads(n.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)

        # clear
        repo.dropPermanent('emsdb')
        repo.createPermanent('emsdb')
        repo['biken_riken.emsdb'].insert_many(r)


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
        
        repo.authenticate('biken_riken', 'biken_riken')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/data/bm181354_rikenm/')
        
        this_script = doc.agent('alg:biken_riken#ems', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:Emergency_Medical_Service_EMS_Stations', {'prov:label':'dataset of medical service in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_ems = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_ems, this_script)
        
        #change this
        doc.usage(get_ems, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})
                  
        ems = doc.entity('dat:biken_riken#emsdb', {prov.model.PROV_LABEL:'Emergency index', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(ems, this_script)
        doc.wasGeneratedBy(ems, get_ems, endTime)
        doc.wasDerivedFrom(ems, resource, get_ems, get_ems, get_ems)
        
                  
        repo.logout()
        return doc

## eof


