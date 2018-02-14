import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json


class uberTravelTimes(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = []
    writes = ['aoconno8_dmak1112.uberTravelTimes']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        url = 'http://datamechanics.io/data/aoconno8_dmak1112/Uber_Travel_Times_Daily.csv'
        uber_dict = pd.read_csv(url).to_dict(orient='records')

        repo.dropCollection("uberTravelTimes")
        repo.createCollection("uberTravelTimes")
        repo['aoconno8_dmak1112.uberTravelTimes'].insert_many(uber_dict)
        repo['aoconno8_dmak1112.uberTravelTimes'].metadata({'complete': True})
        print(repo['aoconno8_dmak1112.uberTravelTimes'].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):


# '''
#            Create the provenance document describing everything happening
#            in this script. Each run of the script will generate a new
#            document describing that invocation event.
#            '''
#
#        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aoconno8_dmak1112') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aoconno8_dmak1112') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:aoconno8_dmak1112#uberTravelTimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Uber_Travel_Times_Daily', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_uberTravelTimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_uberTravelTimes, this_script)
        doc.usage(get_uberTravelTimes, resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval'                 }
                 )

        uberTravelTimes = doc.entity('dat:aoconno8_dmak1112#uberTravelTimes', {prov.model.PROV_LABEL:'Daily Uber Travel Times', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uberTravelTimes, this_script)
        doc.wasGeneratedBy(uberTravelTimes, get_uberTravelTimes, endTime)
        doc.wasDerivedFrom(uberTravelTimes, resource, get_uberTravelTimes, get_uberTravelTimes, get_uberTravelTimes)

        repo.logout()

        return doc

uberTravelTimes.execute()
doc = uberTravelTimes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


