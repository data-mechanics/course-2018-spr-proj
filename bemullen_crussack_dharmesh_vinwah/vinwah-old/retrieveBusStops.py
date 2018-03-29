import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieveBusStops(dml.Algorithm):
    contributor = 'vinwah'
    reads = []
    writes = ['vinwah.busStops']

    @staticmethod
    def execute(trial = False):
        '''
        Retrieve geographical location of busstops
        '''

        startTime = datetime.datetime.now()

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        # End-point associated with data in geojson format
        uri = 'http://datamechanics.io/data/jdbrawn_slarbi/MBTA_Bus_Stops.geojson'
        
        # response from get request
        response = urllib.request.urlopen(uri).read().decode("utf-8")

        # response as json
        r = json.loads(response)

        # remove old collection, and create a new one
        repo.dropCollection("busStops")
        repo.createCollection("busStops")

        # insert data into collection 
        repo['vinwah.busStops'].insert_many(r['features'])

        repo.logout() 

        endTime = datetime.datetime.now()

        print('retrieve busStops finished at:', endTime)

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
        repo.authenticate('vinwah', 'vinwah')

        # set up namespace
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/vinwah#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/vinwah#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('591', 'http://datamechanics.io/data/')

        # Set up agent, entities, activity
        this_script = doc.agent('alg:retrieveBusStps', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('591:jdbrawn_slarbi/MBTA_Bus_Stops', {'prov:label':'Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        bus_stops = doc.entity('dat:busStops', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        
        # establish relationships 
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        doc.wasAttributedTo(bus_stops, this_script)
        doc.wasGeneratedBy(bus_stops, get, endTime)
        doc.wasDerivedFrom(bus_stops, resource, get, get, get)

        repo.logout()
                  
        return doc
