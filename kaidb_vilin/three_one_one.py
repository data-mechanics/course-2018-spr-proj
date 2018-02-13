
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
    DEBUG = True


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
        base = "https://data.boston.gov/"
        api = "api/3/action/datastore_search?resource_id="
        r_id = "2968e2c0-d479-49ba-a884-4ef523ada3c0"
        
        # Size per batch
        l = 1000
        lim = '&limit=' + str(l)
        # initial URL 
        url =  api + r_id+lim 

        #  number of batches
        pages = 10

        data = []
        for i in range(pages): 
            response =  urllib.request.urlopen(base + url ).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            url = r['result']['_links']['next']
            a = r['result']
            print("Adding Batch ", i)
            data.append(r['result']['records'][0])


        repo.dropCollection("three_one_one")
        repo.createCollection("three_one_one")

        repo['kaidb_vilin.three_one_one'].insert_many(data)

        
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
        doc.add_namespace('rc', '311')

        this_script = doc.agent('alg:kaidb_vilin#three_one_one', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_three_one_one = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_three_one_one, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_three_one_one, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'api/3/action/datastore_search?resource_id=2968e2c0-d479-49ba-a884-4ef523ada3c0'
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
