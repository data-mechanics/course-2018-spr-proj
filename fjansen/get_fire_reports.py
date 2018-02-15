import prequest
import json
import dml
import prov.model
import datetime
import uuid


class get_fire_reports(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.fires']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')

        if trial:
            urls = [
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=8f4f497e-d93c-4f2f-b754-bfc69e2700a0&limit=1000'
            ]
        else:
            urls = [
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=8608b9db-71e2-4acb-9691-75b3c66fdd17&limit=10000',
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=d969a70d-2734-4e75-b2ae-e64aec289892&limit=10000',
                'https://data.boston.gov/api/3/action/datastore_search?resource_id=8f4f497e-d93c-4f2f-b754-bfc69e2700a0&limit=10000'
            ]
        responses = []

        for url in urls:
            temp = json.loads(prequest.get(url).text)
            for e in temp['result']['records']:
                # Delete pre-assigned _id, which clashes with mongo
                del e['_id']
            responses.append(temp['result']['records'])

        repo.dropCollection("fires")
        repo.createCollection("fires")
        for response in responses:
            repo['fjansen.fires'].insert_many(response)

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

        this_script = doc.agent('alg:fjansen#get_fire_reports',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:8f4f497e-d93c-4f2f-b754-bfc69e2700a0',
                              {'prov:label': 'Fire Incident Reporting', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('bdp:d969a70d-2734-4e75-b2ae-e64aec289892',
                               {'prov:label': 'Fire Incident Reporting', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        resource3 = doc.entity('bdp:8608b9db-71e2-4acb-9691-75b3c66fdd17',
                               {'prov:label': 'Fire Incident Reporting', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        fires = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(fires, this_script)

        doc.usage(fires, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval',
                                                     'ont:Query': '?resource_id=8f4f497e-d93c-4f2f-b754-bfc69e2700a0&limit=10000'})
        doc.usage(fires, resource2, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval',
                                                      'ont:Query': '?resource_id=d969a70d-2734-4e75-b2ae-e64aec289892&limit=10000'})
        doc.usage(fires, resource3, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval',
                                                      'ont:Query': '?resource_id=8608b9db-71e2-4acb-9691-75b3c66fdd17&limit=10000'})

        output = doc.entity('dat:fjansen#fires',
                            {prov.model.PROV_LABEL: 'Fire Incident Reporting', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, fires, endTime)
        doc.wasDerivedFrom(output, resource, resource2, resource3, fires)

        repo.logout()

        return doc


get_fire_reports.execute()
docs = get_fire_reports.provenance()
print(docs.get_provn())
print(json.dumps(json.loads(docs.serialize()), indent=4))

## eof
