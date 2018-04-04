import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime(dml.Algorithm):
    contributor = 'cma4_lliu_saragl_tsuen'
    reads = []
    writes = ['cma4_lliu_saragl_tsuen.crime']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')

        url = 'http://datamechanics.io/data/20127to20158crimeincident2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        print(response)
        r = json.loads(response)

        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("cma4_lliu_saragl_tsuen.crime")
        repo.createCollection("cma4_lliu_saragl_tsuen.crime")

        final = []

        if trial:
            final = repo['cma4_lliu_saragl_tsuen.entertainment'].aggregate([{'$sample': {'size': 1000}}], allowDiskUse=True)
        else:
            final = repo['cma4_lliu_saragl_tsuen.entertainment'].find()

        repo['cma4_lliu_saragl_tsuen.crime'].insert_many(trial)
        repo['cma4_lliu_saragl_tsuen.crime'].metadata({'complete':True})
        print(repo['cma4_lliu_saragl_tsuen.crime'].metadata())

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
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('hub', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:cma4_lliu_saragl_tsuen#crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:crime', {'prov:label':'crime Station Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        crime = doc.entity('dat:cma4_lliu_saragl_tsuen#crime', {prov.model.PROV_LABEL:'crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc

crime.execute()
doc = crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
