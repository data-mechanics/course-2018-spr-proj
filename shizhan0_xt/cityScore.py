import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class cityScore(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = []
    writes = ['shizhan0_xt.cityScore']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        url = 'https://data.boston.gov/export/edc/24d/edc24d12-4e0d-43dd-a61d-687fc65e2d1b.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cityScore")
        repo.createCollection("cityScore")
        repo['shizhan0_xt.cityScore'].insert_many(r)

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
        doc.add_namespace('bdp', 'https://data.boston.gov/export/edc/')

        this_script = doc.agent('alg:#shizhan0_xt#cityScore', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp:24d', {'prov:label': 'cityScore', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_cityScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cityScore, this_script)
        doc.usage(get_cityScore, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        cityScore = doc.entity('dat:shizhan0_xt#cityScore', {prov.model.PROV_LABEL:'cityScore', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cityScore, this_script)
        doc.wasGeneratedBy(cityScore, get_cityScore, endTime)
        doc.wasDerivedFrom(cityScore, resource, get_cityScore, get_cityScore, get_cityScore)

        repo.logout()

        return doc


# cityScore.execute()
# doc = cityScore.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))