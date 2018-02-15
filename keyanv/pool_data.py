import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class pool_data(dml.Algorithm):
    contributor = 'keyanv'
    reads = []
    writes = ['keyanv.pools']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5575f763dbb64effa36acd67085ef3a8_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("pools")
        repo.createCollection("pools")
        repo['keyanv.pools'].insert_many(r['features'])
        repo['keyanv.pools'].metadata({'complete': True})
        print(repo['keyanv.pools'].metadata())

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

        this_script = doc.agent('alg:keyanv#pool_data', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bod:5575f763dbb64effa36acd67085ef3a8_0', {'prov:label': 'Pool Data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        get_pools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.wasAssociatedWith(get_pools, this_script)
        doc.usage(get_pools, resource, startTime)
        pools = doc.entity('dat:keyanv#pools', {prov.model.PROV_LABEL: 'Pool Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(pools, this_script)
        doc.wasGeneratedBy(pools, get_pools, endTime)
        doc.wasDerivedFrom(pools, resource, get_pools, get_pools, get_pools)

        repo.logout()

        return doc


pool_data.execute()
doc = pool_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof