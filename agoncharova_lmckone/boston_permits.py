import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

class boston_permits(dml.Algorithm):
    contributor = 'agoncharova_lmckone'
    reads = []
    writes = ['agoncharova_lmckone.boston_permits']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Approved Building Permits dataset'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        repo.dropCollection("boston_permits")
        repo.createCollection("boston_permits")

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=6ddcd912-32a0-43df-9908-63574f8c7e77&limit=105750'
        response = ur.urlopen(url)
        data = json.load(response)
        r = data['result']['records']

        print("About to insert " + str(len(r)) + " data points that were fetched")
        repo['agoncharova_lmckone.boston_permits'].insert_many(r)
        repo['agoncharova_lmckone.boston_permits'].metadata({'complete':True})
        print(repo['agoncharova_lmckone.boston_permits'].metadata())

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
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/')

        this_script = doc.agent('alg:agoncharova_lmckone#boston_permits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:6ddcd912-32a0-43df-9908-63574f8c7e77', {'prov:label':'Boston: Approved Building Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_permits, this_script)

        doc.usage(get_boston_permits, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        boston_permits = doc.entity('dat:agoncharova_lmckone#boston_permits', {prov.model.PROV_LABEL:'Boston: Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_permits, this_script)
        doc.wasGeneratedBy(boston_permits, get_boston_permits, endTime)
        doc.wasDerivedFrom(boston_permits, resource, get_boston_permits, get_boston_permits, get_boston_permits)
        repo.logout()
                  
        return doc


boston_permits.execute()
boston_permits.provenance()

## eof
