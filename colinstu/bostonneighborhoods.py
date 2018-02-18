import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class bostonneighborhoods(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.neighborhood']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighborhood")
        repo.createCollection("neighborhood")
        repo['colinstu.neighborhood'].insert_many(r['features'])
        repo['colinstu.neighborhood'].metadata({'complete': True})
        print(repo['colinstu.neighborhood'].metadata())

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
        repo.authenticate('colinstu', 'colinstu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/active-food-establishment-licenses')

        this_script = doc.agent('alg:colinstu#neighborhood',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'Boston Neighborhoods',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_neighborhood = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighborhood, this_script)

        doc.usage(get_neighborhood, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )

        neighborhood = doc.entity('dat:colinstu#neighborhood', {prov.model.PROV_LABEL: 'Boston Neighborhoods',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(neighborhood, this_script)
        doc.wasGeneratedBy(neighborhood, get_neighborhood, endTime)
        doc.wasDerivedFrom(neighborhood, resource, get_neighborhood, get_neighborhood, get_neighborhood)
        repo.logout()

        return doc


bostonneighborhoods.execute()
doc = bostonneighborhoods.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
