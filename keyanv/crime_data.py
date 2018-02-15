import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime_data(dml.Algorithm):
    contributor = 'keyanv'
    reads = []
    writes = ['keyanv.crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        url = 'https://data.boston.gov/export/12c/b38/12cb3883-56f5-47de-afa5-3b1cf61b257b.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        # preprocessing to fix the mistakes of the ignorant soul who provided this data without verifying if it is valid json
        response = response.replace(']', "")
        response += ']'

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crimes")
        repo.createCollection("crimes")
        repo['keyanv.crimes'].insert_many(r)
        repo['keyanv.crimes'].metadata({'complete':True})
        print(repo['keyanv.crimes'].metadata())

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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/')

        this_script = doc.agent('alg:keyanv#crime_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:12c/b38/12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimes, this_script)
        doc.usage(get_crimes, resource, startTime)
        crimes = doc.entity('dat:keyanv#crimes', {prov.model.PROV_LABEL:'Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crimes, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crimes, get_crimes, get_crimes)

        repo.logout()
                  
        return doc

crime_data.execute()
doc = crime_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
