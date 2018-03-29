import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson

class getBikeNetwork(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.bikeNetwork']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get Bike Network data
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        r = gj['features']

        # Store in DB
        repo.dropCollection("bikeNetwork")
        repo.createCollection("bikeNetwork")
        repo['janellc_rstiffel.bikeNetwork'].insert_many(r)
        repo['janellc_rstiffel.bikeNetwork'].metadata({'complete':True})
        print(repo['janellc_rstiffel.bikeNetwork'].metadata())

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
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # Resource:
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:janellc_rstiffel#getBikeNetwork', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:d02c9d2003af455fbc37f550cc53d3a4_0', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_bikeNetwork = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bikeNetwork, this_script)

        doc.usage(get_bikeNetwork, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        bikeNetwork = doc.entity('dat:janellc_rstiffel#bikeNetwork', {prov.model.PROV_LABEL:'Bike Network', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeNetwork, this_script)
        doc.wasGeneratedBy(bikeNetwork, get_bikeNetwork, endTime)
        doc.wasDerivedFrom(bikeNetwork, resource, get_bikeNetwork, get_bikeNetwork, get_bikeNetwork)

        repo.logout()
                  
        return doc

# getBikeNetwork.execute()
# doc = getBikeNetwork.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
