import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

'''In this script I am gathering data on all existing Hubway stations and storing it in a MongoDB collection called hubwayStations.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics'''

class get_hubwayStations(dml.Algorithm):
    contributor = 'pandreah'
    reads = []
    writes = ['pandreah.hubwayStations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')

        url = 'http://datamechanics.io/data/hubwaydump.json'                #This is where the data is being gathered from
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubwayStations")
        repo.createCollection("hubwayStations")
        repo['pandreah.hubwayStations'].insert_many(r)                      #Data being stored in MongoDB
        repo['pandreah.hubwayStations'].metadata({'complete':True})
        print(repo['pandreah.hubwayStations'].metadata())

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
        repo.authenticate('pandreah', 'pandreah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pandreah#hubwayStations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hubwayStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubwayStations, this_script)
        doc.usage(get_hubwayStations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Hubway+Stations&$select=type,OBJECTID,id,name, lat, long, nbBikes, nbEmptyDocks, latestUpdateTime, geometry, coordinates'
                  }
                  )

        hubwayStation = doc.entity('dat:pandreah#hubwayStations', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayStation, this_script)
        doc.wasGeneratedBy(hubwayStation, get_hubwayStations, endTime)
        doc.wasDerivedFrom(hubwayStation, resource, get_hubwayStations, get_hubwayStations, get_hubwayStations)

        repo.logout()
                  
        return doc

get_hubwayStations.execute()
doc = get_hubwayStations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
