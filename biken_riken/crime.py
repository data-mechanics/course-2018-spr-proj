import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class crime(dml.Algorithm):
    contributor = 'biken_riken'
    reads = []
    writes = ['biken_riken.liquor-licenses', 'biken_riken.crime-record','biken_riken.liquor-crime']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('biken_riken', 'biken_riken')
        
        # Dataset01
        url = 'https://data.boston.gov/dataset/47d501bf-8bfa-4076-944f-da0aedb60c8a/resource/aab353c1-c797-4053-a3fc-e893f5ccf547/download/liquor-licenses.csv'
        df = pd.read_csv(url).head(1000)
        df_new = df[df.Location != '(0.0, 0.0)']
        
        columns = ['OPENING','CLOSING','PATRONSOUT','STNOHI','PHONE','STNO','DBANAME','ISSDTTM','EXPDTTM','LICSTATUS','LICCAT','LICCATDESC','PRIMAPPLICANT']
        
        df_new = df_new.drop(columns, axis=1)
        r = json.loads(df_new.to_json(orient='records'))
        # can store this s
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("liquor-licenses")
        repo.createCollection("liquor-licenses")
        
        # stores here
        repo['biken_riken.liquor-licenses'].insert_many(r)
        repo['biken_riken.liquor-licenses'].metadata({'complete':True})
        #print(repo['biken_riken.liquor-licenses'].metadata())
        
        # Dataset02
        url = 'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/crime.csv'
        crime_df = pd.read_csv(url,encoding = "ISO-8859-1").head(1000)
        columns = ['DISTRICT','REPORTING_AREA','SHOOTING','OCCURRED_ON_DATE','Lat','Long','MONTH']
        crime_df = crime_df.drop(columns, axis=1)
        r = json.loads(crime_df.to_json(orient='records'))
        # can store this s
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime-record")
        repo.createCollection("crime-record")
        repo['biken_riken.crime-record'].insert_many(r)
        repo['biken_riken.crime-record'].metadata({'complete':True})
        #print(repo['biken_riken.crime-record'].metadata())
        
        
        #Union of both the datasets:
        combined_db = pd.concat([crime_df,df_new])
        r = json.loads(combined_db.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("liquor-crime")
        repo.createCollection("liquor-crime")
        repo['biken_riken.liquor-crime'].insert_many(r)
        
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
        doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','https://data.boston.gov/dataset/47d501bf-8bfa-4076-944f-da0aedb60c8a/resource/aab353c1-c797-4053-a3fc-e893f5ccf547/download/')
        
        this_script = doc.agent('alg:biken_riken#crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:liquor-licenses', {'prov:label':'dataset of all liquor license in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        resource_two = doc.entity('bdp:crime', {'prov:label':'dataset of all criminal record in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        get_liquor_license = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        get_crime_liquor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_liquor_license, this_script)
        
        # change here
        doc.usage(get_crime, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})
        doc.usage(get_liquor_license, resource_two, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                            })
                  
        liquor_license = doc.entity('dat:biken_riken#liquor-licenses', {prov.model.PROV_LABEL:'liquor license', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(liquor_license, this_script)
        doc.wasGeneratedBy(liquor_license, get_liquor_license, endTime)
        doc.wasDerivedFrom(liquor_license, resource, get_liquor_license, get_liquor_license, get_liquor_license)
                  
        crime = doc.entity('dat:biken_riken#crime-record', {prov.model.PROV_LABEL:'record of all crimes in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        
        ######
        doc.wasAttributedTo(crime, this_script)
        
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_two,  get_crime,  get_crime, get_crime)

        # one more for concatination
        liquor_crime = doc.entity('dat:biken_riken#liquor-crime', {prov.model.PROV_LABEL:'record of all crimes and liquor filtered and unionized', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(liquor_crime, this_script)
        
        doc.wasGeneratedBy(liquor_crime,get_crime_liquor, endTime)
        
        # this change this
        doc.wasDerivedFrom(liquor_crime, resource_two,  get_crime_liquor, get_crime_liquor, get_liquor_license)
        
        repo.logout()
        return doc

## eof

