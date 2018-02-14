import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class propertyvalue(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong.propertyvalue']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Property Value Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=cecdf003-9348-4ddb-94e1-673b63940bb8&limit=20000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        propvalue_json = [json.loads(response)]
        s = json.dumps(propvalue_json, sort_keys=True, indent=2)
        repo.dropCollection("propertyvalue")
        repo.createCollection("propertyvalue")
        repo['ashleyyu_bzwtong.propertyvalue'].insert_many(propvalue_json)
        repo['ashleyyu_bzwtong.propertyvalue'].metadata({'complete':True})
        print(repo['ashleyyu_bzwtong.propertyvalue'].metadata())
        
        repo.logout()
        
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve Property Value Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=cecdf003-9348-4ddb-94e1-673b63940bb8&limit=20000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        propvalue_json = [json.loads(response)]
        s = json.dumps(propvalue_json, sort_keys=True, indent=2)
        repo.dropCollection("propertyvalue")
        repo.createCollection("propertyvalue")
        repo['ashleyyu_bzwtong.propertyvalue'].insert_many(propvalue_json)
        repo['ashleyyu_bzwtong.propertyvalue'].metadata({'complete':True})
        print(repo['ashleyyu_bzwtong.propertyvalue'].metadata())
        
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
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ashleyyu_bzwtong#propertyvalue', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Boston Property Values',
                              {'prov:label': 'Boston Property Values Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_propvalue = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_propvalue, this_script)
        doc.usage(get_propvalue, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        propvalue = doc.entity('dat:ashleyyu_bzwtong#propertyvalue', {prov.model.PROV_LABEL:'Boston Property Values ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propvalue, this_script)
        doc.wasGeneratedBy(propvalue, get_propvalue, endTime)
        doc.wasDerivedFrom(propvalue, resource, get_propvalue, get_propvalue, get_propvalue)

        repo.logout()

        return doc
    
    
doc = propertyvalue.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
