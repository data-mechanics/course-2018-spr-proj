import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = []
    writes = ['shizhan0_xt.crime']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        url = 'http://datamechanics.io/data/shizhan0_xt/crime.json'
        url2 = 'http://datamechanics.io/data/shizhan0_xt/crime2.json'
        url3 = 'http://datamechanics.io/data/shizhan0_xt/crime3.json'
        url4 = 'http://datamechanics.io/data/shizhan0_xt/crime4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response2 = urllib.request.urlopen(url2).read().decode("utf-8")
        response3 = urllib.request.urlopen(url3).read().decode("utf-8")
        response4 = urllib.request.urlopen(url4).read().decode("utf-8")
        r1 = json.loads(response)
        r2 = json.loads(response2)
        r3 = json.loads(response3)
        r4 = json.loads(response4)
        r = r1 + r2 + r3 + r4

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['shizhan0_xt.crime'].insert_many(r)

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt', 'shizhan0_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/shizhan0_xt/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/shizhan0_xt/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:#shizhan0_xt#crime', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp:shizhan0_xt', {'prov:label': 'crime', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        crime = doc.entity('dat:shizhan0_xt#crime', {prov.model.PROV_LABEL:'crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc


# crime.execute()
# doc = crime.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))