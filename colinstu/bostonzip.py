import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class bostonzip(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.bostonzip']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/53ea466a189b4f43b3dfb7b38fa7f3b6_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostonzip")
        repo.createCollection("bostonzip")
        repo['colinstu.bostonzip'].insert_many(r['features'])
        repo['colinstu.bostonzip'].metadata({'complete': True})
        print(repo['colinstu.bostonzip'].metadata())

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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/53ea466a189b4f43b3dfb7b38fa7f3b6_1.geojson')

        this_script = doc.agent('alg:colinstu#bostonzip',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'Boston bostonzip',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_zip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zip, this_script)

        doc.usage(get_zip, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )

        zip = doc.entity('dat:colinstu#bostonzip', {prov.model.PROV_LABEL: 'Boston Zip Codes',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip, this_script)
        doc.wasGeneratedBy(zip, get_zip, endTime)
        doc.wasDerivedFrom(zip, resource, get_zip, get_zip, get_zip)
        repo.logout()

        return doc


bostonzip.execute()
doc = bostonzip.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
