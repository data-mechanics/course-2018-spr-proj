import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class TrafficMovement(dml.Algorithm):
    contributor = 'alyu_sharontj'
    reads = []
    writes = ['alyu_sharontj.TrafficMovement']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')

        # trafficmovement
        url = 'http://datamechanics.io/data/alyu_sharontj/boston_taz.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("TrafficMovement")
        repo.createCollection("TrafficMovement")
        repo['alyu_sharontj.TrafficMovement'].insert_many(r['features'])
        repo['alyu_sharontj.TrafficMovement'].metadata({'complete': True})
        print(repo['alyu_sharontj.TrafficMovement'].metadata())

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
        doc.add_namespace('bdp', 'http://datamechanics.io/data/alyu_sharontj/')

        this_script = doc.agent('alg:alyu_sharontj#TrafficMovement', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})#change to file name
        resource = doc.entity('bdp:boston_taz', {'prov:label':'boston_taz', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_TM = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  #TS= traffic Signals

        doc.wasAssociatedWith(get_TM, this_script)
        doc.usage(get_TM, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                 #'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        TM = doc.entity('dat:alyu_sharontj#TrafficMovement', {prov.model.PROV_LABEL:'Traffic Movement', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TM, this_script)
        doc.wasGeneratedBy(TM, get_TM, endTime)
        doc.wasDerivedFrom(TM, resource, get_TM, get_TM, get_TM)


        repo.logout()

        return doc

TrafficMovement.execute()
doc = TrafficMovement.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
