import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class gdpUS(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = []
    writes = ['shizhan0_xt.gdpUS']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        url = 'https://api.db.nomics.world/api/v1/json/series/worldbank-gem-gdp-at-market-prices-current-us-millions-seas-adj-united-states-annually'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r = r['data']['values']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("gdpUS")
        repo.createCollection("gdpUS")
        repo['shizhan0_xt.gdpUS'].insert_many(r)

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
        doc.add_namespace('bdp', 'https://api.db.nomics.world/api/v1/json/series/')

        this_script = doc.agent('alg:#shizhan0_xt#gdpUS', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp:worldbank-gem-gdp-at-market-prices-current-us-millions-seas-adj-united-states-annually', {'prov:label': 'gdpUS', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_gdpUS = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_gdpUS, this_script)
        doc.usage(get_gdpUS, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        gdpUS = doc.entity('dat:shizhan0_xt#gdpUS', {prov.model.PROV_LABEL:'gdpUS', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(gdpUS, this_script)
        doc.wasGeneratedBy(gdpUS, get_gdpUS, endTime)
        doc.wasDerivedFrom(gdpUS, resource, get_gdpUS, get_gdpUS, get_gdpUS)

        repo.logout()

        return doc


# gdpUS.execute()
# doc = gdpUS.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))