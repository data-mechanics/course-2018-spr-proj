import json
import dml
import prov.model
import datetime
import uuid
from fjansen.utils import utils


class transform_fire_311(dml.Algorithm):
    contributor = 'fjansen'
    reads = ['fjansen.fires', 'fjansen.nyc311']
    writes = ['fjansen.nyc311_fire']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')

        fires = repo['fjansen.fires']
        nyc311 = repo['fjansen.nyc311']

        # Select all dates and fire types, add 1 to end to ease sum later
        fires_date_type = utils.project(fires.find(),
                                        lambda t: (t['Alarm Date'], (t['Incident Description'].strip(), 1)))

        # Group by date
        fires_by_date = utils.aggregate(fires_date_type, lambda x: x)
        # Sum types by date
        fires_by_type_date = utils.project(fires_by_date, lambda x: [x[0], utils.aggregate(x[1], sum)])

        repo.dropCollection('nyc311_fire')
        repo.createCollection('nyc311_fire')
        for e in fires_by_type_date:
            # Convert to object MongoDB can insert
            obj = {'date': e[0], 'incidents': dict(e[1])}
            repo['fjansen.nyc311_fire'].insert(obj)

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

        this_script = doc.agent('alg:fjansen#transform_fire_311',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_fires = doc.entity('dat:fjansen#fires',
                                    {'prov:label': 'Fire Incident Reporting', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_nyc311 = doc.entity('dat:fjansen#incidents',
                                     {'prov:label': '311 Incidence Reports', prov.model.PROV_TYPE: 'ont:DataSet'})

        nyc311_fire = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(nyc311_fire, this_script)

        doc.usage(nyc311_fire, resource_fires, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(nyc311_fire, resource_nyc311, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        output = doc.entity('dat:fjansen#reports_by_date',
                            {prov.model.PROV_LABEL: 'Number of fire and 311 reports by date',
                             prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, nyc311_fire, endTime)
        doc.wasDerivedFrom(output, resource_fires, nyc311_fire)
        doc.wasDerivedFrom(output, resource_nyc311, nyc311_fire)

        repo.logout()

        return doc


transform_fire_311.execute()
docs = transform_fire_311.provenance()
print(docs.get_provn())
print(json.dumps(json.loads(docs.serialize()), indent=4))

## eof
