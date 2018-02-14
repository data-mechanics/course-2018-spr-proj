import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class gas(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = []
    writes = ['shizhan0_xt.gas']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        url = 'http://datamechanics.io/data/shizhan0_xt/greenhouse.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("gas")
        repo.createCollection("gas")
        repo['shizhan0_xt.gas'].insert_many(r)

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

        this_script = doc.agent('alg:#shizhan0_xt#gas', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp:shizhan0_xt', {'prov:label': 'gas', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_gas = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_gas, this_script)
        doc.usage(get_gas, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        gas = doc.entity('dat:shizhan0_xt#gas', {prov.model.PROV_LABEL:'gas', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(gas, this_script)
        doc.wasGeneratedBy(gas, get_gas, endTime)
        doc.wasDerivedFrom(gas, resource, get_gas, get_gas, get_gas)

        repo.logout()

        return doc


#gas.execute()
#doc = gas.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))