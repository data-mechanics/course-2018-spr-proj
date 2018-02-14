import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class parkingData(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong.parkingData']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=8d38cc9d-8c58-462e-b2df-b793e9c05612&limit=572'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        parking_json = [json.loads(response)]
        repo.dropCollection("parkingData")
        repo.createCollection("parkingData")
        repo['ashleyyu_bzwtong.parkingData'].insert_many(parking_json)
        repo['ashleyyu_bzwtong.parkingData'].metadata({'complete': True})
        print(repo['ashleyyu_bzwtong.parkingData'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aoconno8_dmak1112')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aoconno8_dmak1112')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:ashleyyu_bzwtong#parkingData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:Cars Parked Across All Zones - 2015',
                              {'prov:label': '2015 Parking Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_parkingData = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_parkingData, this_script)
        doc.usage(get_parkingData, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        parkingData = doc.entity('dat:ashleyyu_bzwtong#parkingData',
                                   {prov.model.PROV_LABEL: 'Parking Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(parkingData, this_script)
        doc.wasGeneratedBy(parkingData, get_parkingData, endTime)
        doc.wasDerivedFrom(parkingData, resource, get_parkingData, get_parkingData, get_parkingData)

        repo.logout()

        return doc


parkingData.execute()
doc = parkingData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
