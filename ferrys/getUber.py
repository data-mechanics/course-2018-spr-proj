import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json

class getUber(dml.Algorithm):
    contributor = 'ferrys'
    reads = []
    writes = ['ferrys.uber']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')

        url = 'http://datamechanics.io/data/ferrys/Uber_Travel_Times.csv'
        uber_dict = pd.read_csv(url).to_dict(orient='records')
        
        repo.dropCollection("uber")
        repo.createCollection("uber")
        repo['ferrys.uber'].insert_many(uber_dict)
        repo['ferrys.uber'].metadata({'complete':True})
        print(repo['ferrys.uber'].metadata())

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
        repo.authenticate('ferrys', 'ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ferrys', 'http://datamechanics.io/data/ferrys/')

        this_script = doc.agent('alg:ferrys#getUber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('ferrys:uber', {'prov:label':'Uber Travel Times Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_uber = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_uber, this_script)
        doc.usage(get_uber, resource, startTime, None,
                  {
                    prov.model.PROV_TYPE:'ont:Retrieval'
                  })


        uber_travel = doc.entity('dat:ferrys#uber', {prov.model.PROV_LABEL:'uber', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber_travel, this_script)
        doc.wasGeneratedBy(uber_travel, get_uber, endTime)
        doc.wasDerivedFrom(uber_travel, resource, get_uber, get_uber, get_uber)

        repo.logout()
                  
        return doc

#getUber.execute()
#doc = getUber.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

