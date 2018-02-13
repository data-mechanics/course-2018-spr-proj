import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class violations(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = []
    writes = ['lliu_saragl.violations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=90ed3816-5e70-443c-803d-9a71f44470be&q=snow'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)
        repo.dropPermanent("violations")
        repo.createPermanent("violations")
        
        repo['lliu_saragl.violations'].insert_many(r['result']['records'])
        repo['lliu_saragl.violations'].metadata({'complete':True})
        print(repo['lliu_saragl.violations'].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenacne document describing everything happening in this script.
        Each run of the script will generate a new document describing that invocation event.
        '''

        # Set up the database connection 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:lliu_saragl#violations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:90ed3816-5e70-443c-803d-9a71f44470be', {'prov:label': 'List of Snow Violations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_violations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_violations, this_script)
        
        doc.usage(get_violations, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        violations = doc.entity('dat:lliu_saragl#violations',{prov.model.PROV_LABEL:'List of Snow Violations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(violations, this_script)
        doc.wasGeneratedBy(violations, get_violations, endTime)
        doc.wasDerivedFrom(violations, get_violations, get_violations, get_violations)

        repo.logout()

        return doc

violations.execute()
doc = violations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
