import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crashdata(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong', 'ashleyyu_bzwtong']

    @staticmethod
    def execute(trial = False):
        '''Crash Data in Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://datamechanics.io/data/crash.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        crash_json = json.loads(response)
        repo.dropCollection("crashdata")
        repo.createCollection("crashdata")
        repo['ashleyyu_bzwtong'].insert_many(crash_json)
        repo['ashleyyu_bzwtong'].metadata({'complete':True})
        print(repo['ashleyyu_bzwtong'].metadata())

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

        this_script = doc.agent('alg:ashleyyu_bzwtong#crashdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crash = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash, this_script)
        doc.usage(get_crash, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Crash+Data&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        crash = doc.entity('dat:ashleyyu_bzwtong#crashdata', {prov.model.PROV_LABEL:'Crash Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crash, this_script)
        doc.wasGeneratedBy(crash, get_crash, endTime)
        doc.wasDerivedFrom(crash, resource, get_crash, get_crash, get_crash)

        repo.logout()
                  
        return doc

crashdata.execute()
doc = crashdata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
