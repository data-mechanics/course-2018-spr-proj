import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json

class getAlcLicenses(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = []
    writes = ['aoconno8_dmak1112_ferrys.alc_licenses']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')

        url = 'https://data.boston.gov/dataset/df9987bb-3459-4594-9764-c907b53f2abe/resource/9e15f457-1923-4c12-9992-43ba2f0dd5e5/download/all-section-12-alcohol-licenses.csv'
        license_dict = pd.read_csv(url).to_dict(orient='records')
        
        repo.dropCollection("alc_licenses")
        repo.createCollection("alc_licenses")
        repo['aoconno8_dmak1112_ferrys.alc_licenses'].insert_many(license_dict)
        repo['aoconno8_dmak1112_ferrys.alc_licenses'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.alc_licenses'].metadata())

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
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getAlcLicenses', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:all-section-12-alcohol-licenses', {'prov:label':'Alcohol License Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_licenses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_licenses, this_script)
        doc.usage(get_licenses, resource, startTime, None,
                  {
                    prov.model.PROV_TYPE:'ont:Retrieval'
                  })


        licenses = doc.entity('dat:aoconno8_dmak1112_ferrys#alc_licenses', {prov.model.PROV_LABEL:'alc_licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(licenses, this_script)
        doc.wasGeneratedBy(licenses, get_licenses, endTime)
        doc.wasDerivedFrom(licenses, resource, get_licenses, get_licenses, get_licenses)

        repo.logout()
                  
        return doc

#getAlcLicenses.execute()
#doc = getAlcLicenses.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

