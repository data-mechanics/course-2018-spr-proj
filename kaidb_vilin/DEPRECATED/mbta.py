
import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
import os



def find(name, path): #Helper method for locating files
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


class mbta(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.mbta']
    DEBUG = False


    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')

        path = "../"
        auth_path = find('auth.json', path)
        #print(auth_path)
        with open(auth_path) as json_file:
            key = json.load(json_file)
            api_key = key['mbta_api_key']

        api_url = "https://api-v3.mbta.com/"
        added_key = 'routes?api_key='+api_key+'&format=json'

        req_url = api_url + added_key
        print(req_url)
        response = urllib.request.urlopen(req_url).read().decode("utf-8")
        r = json.loads(response)['data']
                    
        repo.dropCollection("mbta")
        repo.createCollection("mbta")

        #if mbta.DEBUG:
         #   print(type(r))
          #  assert isinstance(r, dict)

        repo['kaidb_vilin.mbta'].insert_many(r)

        repo['kaidb_vilin.mbta'].metadata({'complete':True})
        print(repo['kaidb_vilin.mbta'].metadata())

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
        # Your API key here
        doc.add_namespace('dbd', 'routes?api_key=&format=json')
        doc.add_namespace('rc', 'employee-earnings-report-2016')

        this_script = doc.agent('alg:kaidb_vilin#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_mbta = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_mbta, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'routes?api_key=&format=json'
                  }
                  )
     

        mbta = doc.entity('dat:kaidb_vilin#mbta', {prov.model.PROV_LABEL:'mbta', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbta, endTime)
        doc.wasDerivedFrom(mbta, resource, get_mbta, get_mbta, get_mbta)
        repo.logout()
                  
        return doc

# comment this out for submission. 
mbta.execute()
doc = mbta.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
