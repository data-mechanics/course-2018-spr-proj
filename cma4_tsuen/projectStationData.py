import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class projectDestinationData(dml.Algorithm):
    contributor = 'cma4_tsuen'
    reads = ['cma4_tsuen.mbta', 'cma4_tsuen.hubway']
    writes = ['cma4_tsuen.stationsProjected']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')

        dataSet = []

        collection = repo['cma4_tsuen.hubway'].find()

        # projection
        dataSet = [
        	{'name': row["s"],
        	'coords': (row["la"], row["lo"])}
        	for row in collection
        ]

        collection2 = repo['cma4_tsuen.mbta']

        dataSet.append({
        	'name': row['businessName'],
        	'coords': row['Location']
        	} for row in collection2
            )

        final = []
        for entry in dataSet:
            if entry not in final:
                final.append(entry)

        print(final)

        repo.dropCollection("cma4_tsuen.stationsProjected")
        repo.createCollection("cma4_tsuen.stationsProjected")
        repo['cma4_tsuen.stationsProjected'].insert_many(final)
        repo['cma4_tsuen.stationsProjected'].metadata({'complete':True})
        print(repo['cma4_tsuen.stationsProjected'].metadata())

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
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('hubway', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:cma4_tsuen#hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        lost = doc.entity('dat:cma4_tsuen#hubway', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_stations, endTime)
        doc.wasDerivedFrom(hubway, resource, get_stations, get_stations, get_stations)

        repo.logout()
                  
        return doc

projectDestinationData.execute()
doc = projectDestinationData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof