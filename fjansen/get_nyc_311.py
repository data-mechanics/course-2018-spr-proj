import prequest
import json
import dml
import prov.model
import datetime
import uuid


class GetNYC311(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.nyc311']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')
        auth_key = dml.auth['services']['nycportal']['token']

        url = 'https://data.cityofnewyork.us/resource/fhrw-4uyv.json'
        if trial:
            params = {'$limit': 1000, '$$app_token': auth_key}
        else:
            params = {'$limit': 10000, '$$app_token': auth_key}

        resp = json.loads(prequest.get(url, params=params).text)

        repo.dropCollection('nyc311')
        repo.createCollection('nyc311')
        repo['fjansen.nyc311'].insert_many(resp)

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
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        this_script = doc.agent('alg:fjansen#get_nyc_311',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('nyc:fhrw-4uyv',
                              {'prov:label': '311 Incidence Reports', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        incidents = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(incidents, this_script)

        doc.usage(incidents, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval',
                                                         'ont:Query': '?limit=10000'})

        output = doc.entity('dat:fjansen#incidents',
                            {prov.model.PROV_LABEL: '311 Incidence Reports', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, incidents, endTime)
        doc.wasDerivedFrom(output, resource, incidents)

        repo.logout()

        return doc


GetNYC311.execute()
docs = GetNYC311.provenance()
print(docs.get_provn())
print(json.dumps(json.loads(docs.serialize()), indent=4))

## eof
