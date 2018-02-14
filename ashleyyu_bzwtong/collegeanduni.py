import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class collegeanduni(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong.collegeanduni']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        print(response)
        schools_json = [json.loads(response)]
        repo.dropCollection("collegeanduni")
        repo.createCollection("collegeanduni")
        repo['ashleyyu_bzwtong.collegeanduni'].insert_many(schools_json)
        repo['ashleyyu_bzwtong.collegeanduni'].metadata({'complete': True})
        print(repo['ashleyyu_bzwtong.collegeanduni'].metadata())

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
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ashleyyu_bzwtong')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ashleyyu_bzwtong')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:ashleyyu_bzwtong#schools', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:College and Uni in Boston',
                              {'prov:label': 'College and Uni Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_schools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        collegeanduni = doc.entity('dat:ashleyyu_bzwtong#schools',
                                   {prov.model.PROV_LABEL: 'College and Uni', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(collegeanduni, this_script)
        doc.wasGeneratedBy(collegeanduni, get_schools, endTime)
        doc.wasDerivedFrom(collegeanduni, resource, get_schools, get_schools, get_schools)

        repo.logout()

        return doc


collegeanduni.execute()
doc = collegeanduni.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
