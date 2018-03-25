import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class TrafficSignals(dml.Algorithm):
    contributor = 'alyu_sharontj'
    reads = []
    writes = ['alyu_sharontj.TrafficSignals'] #???

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')
        #http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #features=r['features']
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("TrafficSignals") #name of the data link: e.g. station_links
        repo.createCollection("TrafficSignals")
        repo['alyu_sharontj.TrafficSignals'].insert_many(r['features'])    #insert data into database?
        repo['alyu_sharontj.TrafficSignals'].metadata({'complete':True})
        print(repo['alyu_sharontj.TrafficSignals'].metadata())

        # url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("found")
        # repo.createCollection("found")
        # repo['alice_bob.found'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}
    
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
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:alyu_sharontj#TrafficSignals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})#change to file name
        resource = doc.entity('bdp:de08c6fe69c942509089e6db98c716a3_0', {'prov:label':'Traffic Signals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_TS = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  #TS= traffic Signals

        doc.wasAssociatedWith(get_TS, this_script)
        doc.usage(get_TS, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                 # 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        TS = doc.entity('dat:alyu_sharontj#TrafficSignals', {prov.model.PROV_LABEL:'Traffic Signals Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TS, this_script)
        doc.wasGeneratedBy(TS, get_TS, endTime)
        doc.wasDerivedFrom(TS, resource, get_TS, get_TS, get_TS)


        repo.logout()
                  
        return doc

# TrafficSignals.execute()
# doc = TrafficSignals.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
