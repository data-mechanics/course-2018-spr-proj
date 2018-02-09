import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta(dml.Algorithm):
    contributor = 'charles_tommy'
    reads = []
    writes = ['charles_tommy.mbta']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('charles_tommy', 'charles_tommy')

        with open('auth.json') as json_file:
            key = json.load(json_file)
        api_key = key['MBTA_API_KEY']

        url = 'http://realtime.mbta.com/developer/api/v2/routes?api_key='+api_key+'&format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("charles_tommy.mbta")
        repo.createCollection("charles_tommy.mbta")
        repo['charles_tommy.mbta'].insert_many(r)
        repo['charles_tommy.mbta'].metadata({'complete':True})
        print(repo['charles_tommy.mbta'].metadata())

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
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

        this_script = doc.agent('alg:charles_tommy#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=mbta+stops&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:charles_tommy#mbta', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_stops, endTime)
        doc.wasDerivedFrom(mbta, resource, get_stops, get_stops, get_stops)

        repo.logout()
                  
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
