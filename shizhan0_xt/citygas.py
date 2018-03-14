import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class citygas(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = ['shizhan0_xt.cityScore','shizhan0_xt.gas']
    writes = ['shizhan0_xt.citygas']

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        score = [(x['ETL_LOAD_DATE'][0:4],x['CTY_SCR_DAY_AGG']) for x in repo['shizhan0_xt.cityScore'].find()]
        gas = [(x['Year (Fiscal Year)'], x['GHG Emissions (t CO2e)']) for x in repo['shizhan0_xt.gas'].find()]
        avgScore = {}
        yearGas = {}

        for (year, value) in score:
            if(year == '2018'):
                pass
            else:
                total = float(avgScore.get(year, 0)) + float(value)
                avgScore[year] = total

        for (y, v) in gas:
            v = v.replace('-','0')
            v = v.replace(',', '')
            totalgas = float(yearGas.get(y, 0)) + float(v)
            yearGas[y] = totalgas

        citygas = []

        for x in avgScore.keys():
            citygas.append({"Year": x, "AvgCityScore": avgScore[x], "GasEmission": yearGas[x], "Coefficient": avgScore[x]/yearGas[x]})

        repo.dropCollection("citygas")
        repo.createCollection("citygas")
        repo['shizhan0_xt.citygas'].insert_many(citygas)

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

        this_script = doc.agent('alg:#shizhan0_xt#citygas', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        gas = doc.entity('dat:shizhan0_xt#gas', {'prov:label': 'gas', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        cityScore = doc.entity('dat: dat:shizhan0_xt#cityScore',
                               {'prov:label': 'cityScore', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        get_citygas = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_citygas, this_script)

        doc.usage(get_citygas, gas, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_citygas, cityScore, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

        citygas = doc.entity('dat:shizhan0_xt#citygas',
                             {prov.model.PROV_LABEL: 'citygas', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(citygas, this_script)
        doc.wasGeneratedBy(citygas, get_citygas, endTime)

        doc.wasDerivedFrom(citygas, gas, get_citygas, get_citygas, get_citygas)
        doc.wasDerivedFrom(citygas, cityScore, get_citygas, get_citygas, get_citygas)

        repo.logout()

        return doc



# citygas.execute()
# doc = citygas.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))