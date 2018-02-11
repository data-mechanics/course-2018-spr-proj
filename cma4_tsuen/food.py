import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class food(dml.Algorithm):
    contributor = 'charles_tommy'
    reads = []
    writes = ['charles_tommy.food']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('charles_tommy', 'charles_tommy')            

        url = 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("charles_tommy.food")
        repo.createCollection("charles_tommy.food")
        repo['charles_tommy.food'].insert_many(r)
        repo['charles_tommy.food'].metadata({'complete':True})
        print(repo['charles_tommy.food'].metadata())

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
        repo.authenticate('charles_tommy', 'charles_tommy')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('food', 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json')

        this_script = doc.agent('alg:charles_tommy#food', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        lost = doc.entity('dat:charles_tommy#food', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(food, this_script)
        doc.wasGeneratedBy(food, get_stops, endTime)
        doc.wasDerivedFrom(food, resource, get_stops, get_stops, get_stops)

        repo.logout()
                  
        return doc

food.execute()
doc = food.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof