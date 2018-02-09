import dml
import prov.model
import datetime
import uuid
import pandas as pd

class getAlcLicenses(dml.Algorithm):
    contributor = 'ferrys'
    reads = []
    writes = ['ferrys.alc_licenses']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')

        url = 'https://data.boston.gov/dataset/df9987bb-3459-4594-9764-c907b53f2abe/resource/9e15f457-1923-4c12-9992-43ba2f0dd5e5/download/all-section-12-alcohol-licenses.csv'
        license_dict = pd.read_csv(url).to_dict(orient='records')
        
        print(license_dict)

        repo.dropCollection("alc_licenses")
        repo.createCollection("alc_licenses")
        repo['ferrys.alc_licenses'].insert_many(license_dict)
        repo['ferrys.alc_licenses'].metadata({'complete':True})
        print(repo['ferrys.alc_licenses'].metadata())

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
#        '''
#            Create the provenance document describing everything happening
#            in this script. Each run of the script will generate a new
#            document describing that invocation event.
#            '''
#
#        # Set up the database connection.
#        client = dml.pymongo.MongoClient()
#        repo = client.repo
#        repo.authenticate('alice_bob', 'alice_bob')
#        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
#
#        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#        doc.wasAssociatedWith(get_found, this_script)
#        doc.wasAssociatedWith(get_lost, this_script)
#        doc.usage(get_found, resource, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval',
#                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
#                  }
#                  )
#        doc.usage(get_lost, resource, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval',
#                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
#                  }
#                  )
#
#        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
#        doc.wasAttributedTo(lost, this_script)
#        doc.wasGeneratedBy(lost, get_lost, endTime)
#        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
#
#        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
#        doc.wasAttributedTo(found, this_script)
#        doc.wasGeneratedBy(found, get_found, endTime)
#        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
#
#        repo.logout()
#                  
#        return doc

getAlcLicenses.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

