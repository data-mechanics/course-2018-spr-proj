import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class crimerate(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong.crimerate']

    @staticmethod
    def execute(trial=False):
        '''Crime Rate in Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://datamechanics.io/data/ashleyyu_bzwtong/crime.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
<<<<<<< HEAD:ashleyyu_bzwtong_xhug/crimerate.py
        crime_json = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
=======
        # print(type(response))
        # print(response[:2000])
        # crimeData = []
        # for objects in response:
        #     crimeData.append(objects)
        #with open('response', 'r') as crimeData:
            #crime = crimeData.read()
        crime_json = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        # print(type(crimeData))
        # print(crimeData[:10])
>>>>>>> e908f49301ef2b4a9cbbb32cd06ca852718f1f58:ashleyyu_bzwtong_xhug/crimerate.py
        repo.dropCollection("crimerate")
        repo.createCollection("crimerate")
        repo['ashleyyu_bzwtong.crimerate'].insert_many(crime_json)
        repo['ashleyyu_bzwtong.crimerate'].metadata({'complete': True})
        print(repo['ashleyyu_bzwtong.crimerate'].metadata())

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

        this_script = doc.agent('alg:ashleyyu_bzwtong#crime', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:Crime Rate in Boston',
                              {'prov:label': 'Crime Rate Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_crime = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        crimerate = doc.entity('dat:ashleyyu_bzwtong#crime',
                                   {prov.model.PROV_LABEL: 'Crime Rate', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crimerate, this_script)
        doc.wasGeneratedBy(crimerate, get_crime, endTime)
        doc.wasDerivedFrom(crimerate, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc


crimerate.execute()
doc = crimerate.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
