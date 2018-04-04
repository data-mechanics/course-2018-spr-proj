import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class policestation(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong_xhug'
    reads = []
    writes = ['ashleyyu_bzwtong_xhug.policestation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Police Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policestation")
        repo.createCollection("policestation")
        repo['ashleyyu_bzwtong.policestation'].insert_one(r)
        print('Load policestation')
        for entry in repo.ashleyyu_bzwtong.policestation.find():
             print(entry)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def execute(trial = True):
        '''Retrieve Police Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policestation")
        repo.createCollection("policestation")
        repo['ashleyyu_bzwtong.policestation'].insert_one(r)
        print('Load policestation')
        for entry in repo.ashleyyu_bzwtong.policestation.find():
            print(entry)
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
        doc.add_namespace('bdp', 'https://data.boston.gov/export/622/208/')

        this_script = doc.agent('alg:ashleyyu_bzwtong#policestation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:e5a0066d38ac4e2abbc7918197a4f6af_6', {'prov:label':'Boston police stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_policestation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_policestation, this_script)
        doc.usage(get_policestation, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        policestation = doc.entity('dat:ashleyyu_bzwtong#policestation', {prov.model.PROV_LABEL:'Boston police stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policestation, this_script)
        doc.wasGeneratedBy(policestation, get_policestation, endTime)
        doc.wasDerivedFrom(policestation, resource, get_policestation, get_policestation, get_policestation)

        repo.logout()

        return doc


#publicschools.execute()
#doc = publicschools.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof