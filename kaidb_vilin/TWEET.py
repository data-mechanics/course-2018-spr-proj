
import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
import sys
import urllib.request
import gzip

import requests, zipfile, io

class TWEET(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.TWEET']
    DEBUG = False

def download_zip(zip_file_url):
    print("Downloading the zip file at {}".format(zip_file_url))
    r = requests.get(zip_file_url)
    print("Extracting contents to local directory")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    

    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')

        # location of twitter data
        dataset_url = "http://thinknook.com/wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip"
        download_zip(dataset_url)

        # retrieve data using pandas 
        data = pd.read_csv("Sentiment Analysis Dataset.csv", index_col=0)
        # puts data in useable format 
        # projects out several columns
        # Adds new columns to measure time-delta for completed taks. 
        data = TWEETclean_transform_data(data)

        # Obj transformation: 
        #  df --> string (json formated) --> json 
        r = json.loads(data.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        # formated string. 
        #s = json.dumps(r, sort_keys=True, indent=2)

        ######

        repo.dropCollection("TWEET")
        repo.createCollection("TWEET")

        repo['kaidb_vilin.TWEET'].insert_many(r)

        
        repo['kaidb_vilin.TWEET'].metadata({'complete':True})
        print(repo['kaidb_vilin.TWEET'].metadata())

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
        doc.add_namespace('dbd', 'http://thinknook.com/')
        doc.add_namespace('rc', 'wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip')

        this_script = doc.agent('alg:kaidb_vilin#TWEET', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_TWEET = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_TWEET, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_TWEET, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip'
                  }
                  )
     

        TWEET = doc.entity('dat:kaidb_vilin#TWEET', {prov.model.PROV_LABEL:'TWEET', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TWEET, this_script)
        doc.wasGeneratedBy(TWEET, get_TWEET, endTime)
        doc.wasDerivedFrom(TWEET, resource, get_TWEET, get_TWEET, get_TWEET)
        repo.logout()
                  
        return doc

# comment this out for submission. 
TWEET.execute()
doc = TWEET.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
