import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class open_space_data(dml.Algorithm):
    contributor = 'keyanv'
    reads = []
    writes = ['keyanv.open_spaces']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("open_spaces")
        repo.createCollection("open_spaces")
        repo['keyanv.open_spaces'].insert_many(r['features'])
        repo['keyanv.open_spaces'].metadata({'complete': True})
        print(repo['keyanv.open_spaces'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:keyanv#open_spaces', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7', {'prov:label': 'Open Spaces', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        get_open_spaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.wasAssociatedWith(get_open_spaces, this_script)
        doc.usage(get_open_spaces, resource, startTime)
        open_spaces = doc.entity('dat:keyanv#open_spaces', {prov.model.PROV_LABEL: 'Open Spaces', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(open_spaces, this_script)
        doc.wasGeneratedBy(open_spaces, get_open_spaces, endTime)
        doc.wasDerivedFrom(open_spaces, resource, get_open_spaces, get_open_spaces, get_open_spaces)

        repo.logout()

        return doc


open_space_data.execute()
doc = open_space_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
