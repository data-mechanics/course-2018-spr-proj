import json
import dml
import prov.model
import datetime
import uuid


class get_311(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.k311']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')

        if trial:
            urls = [
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=2968e2c0-d479-49ba-a884-4ef523ada3c0&limit=5'
            ]
        else:
            urls = [
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=2968e2c0-d479-49ba-a884-4ef523ada3c0&limit=10'
            ]
        responses = []

        for url in urls:
            # The 311 file from Boston is malformed
            with open('/Users/fjansen/Desktop/311.json', 'r') as f:
                temp = json.load(f)

        repo.dropCollection("k311")
        repo.createCollection("k311")
        repo['fjansen.k311'].insert_many(temp)

        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/3/action/')

        this_script = doc.agent('alg:fjansen#get_311_reports',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        k311 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(k311, this_script)

        doc.usage(k311, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval',
                                          'ont:Query': '?resource_id=8f4f497e-d93c-4f2f-b754-bfc69e2700a0&limit=10000'})

        output = doc.entity('dat:fjansen#k311',
                            {prov.model.PROV_LABEL: '311 Incident Reporting', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, k311, endTime)

        repo.logout()

        return doc


get_311.execute()
docs = get_311.provenance()
print(docs.get_provn())
print(json.dumps(json.loads(docs.serialize()), indent=4))

## eof
