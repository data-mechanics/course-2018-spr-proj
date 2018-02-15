
import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid

class three_one_one(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.three_one_one']
    DEBUG = False


    @staticmethod
    def clean_transform_data(df):
        """Project data, and clean empty values"""
        # Features used to determine the 
        # Startic features of interest. 
        # Locational features 
        location_columns = ['precinct', 'LOCATION_ZIPCODE', 'Latitude', 'Longitude']
        # complaint types 
        descriptor = ['TYPE', 'REASON']
        # date time objects 
        date_cols = ['open_dt', 'target_dt','closed_dt']
        # Note ontime doesn't mean the case is closed. 
        # It means it is slated to finish before the estimate
        punctual = ['OnTime_Status', 'CASE_STATUS']
        # unique identifier 
        identif = ['CASE_ENQUIRY_ID']

        # Transfrom datatype of features to Date time objects
        df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')
        # time to complete the task 
        # will create NAAN values for open tasks 
        completion_time = df['closed_dt'] - df['open_dt']
        # Deviation from predicted allotment of time
        estimate_deviation = df['closed_dt'] - df['target_dt']
        
        # Project out unecesarry meta-data
        data = df[identif + date_cols + descriptor + location_columns + punctual]
        data['completion_time'] = completion_time

        # Each case is given an estimated required date of completion
        # This computes the difference in actual execution time and estimated 
        data['deviation_from_estimate'] = estimate_deviation
        return data

    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')

        # Updated daily

        url_311 = "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0/download/311.csv"

        # retrieve data using pandas 
        data = pd.read_csv(url_311, encoding = "ISO-8859-1")
        # puts data in useable format 
        # projects out several columns
        # Adds new columns to measure time-delta for completed taks. 
        data = three_one_one.clean_transform_data(data)

        # Obj transformation: 
        #  df --> string (json formated) --> json 
        r = json.loads(data.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        # formated string. 
        #s = json.dumps(r, sort_keys=True, indent=2)

        ######

        repo.dropCollection("three_one_one")
        repo.createCollection("three_one_one")

        repo['kaidb_vilin.three_one_one'].insert_many(r)

        
        repo['kaidb_vilin.three_one_one'].metadata({'complete':True})
        print(repo['kaidb_vilin.three_one_one'].metadata())

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
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dbd', 'https://data.boston.gov/dataset/')
        doc.add_namespace('rc', '8048697b-ad64-4bfc-b090-ee00169f2323/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0/download/311')

        this_script = doc.agent('alg:kaidb_vilin#three_one_one', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_three_one_one = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_three_one_one, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_three_one_one, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'8048697b-ad64-4bfc-b090-ee00169f2323/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0/download/311.csv'
                  }
                  )
     

        three_one_one = doc.entity('dat:kaidb_vilin#three_one_one', {prov.model.PROV_LABEL:'three_one_one', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(three_one_one, this_script)
        doc.wasGeneratedBy(three_one_one, get_three_one_one, endTime)
        doc.wasDerivedFrom(three_one_one, resource, get_three_one_one, get_three_one_one, get_three_one_one)
        repo.logout()
                  
        return doc

# comment this out for submission. 
three_one_one.execute()
doc = three_one_one.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
