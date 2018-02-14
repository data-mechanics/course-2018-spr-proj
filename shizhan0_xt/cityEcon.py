import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class cityEcon(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = ['shizhan0_xt.cityScore','shizhan0_xt.pcpi', 'shizhn0_xt.gdpUS']
    writes = ['shizhan0_xt.cityEcon']


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        score = [(x['ETL_LOAD_DATE'][0:4],x['CTY_SCR_DAY_AGG']) for x in repo['shizhan0_xt.cityScore'].find()]
        keys = {r[0] for r in score}
        score = [(key, sum([float(v) for (k, v) in score if k == key])) for key in keys]

        pcpi = [(x['DATE'][4:], x['BOST625PCPI']) for x in repo['shizhan0_xt.pcpi'].find()]

        gdpUS = [(x['period'], x['value']) for x in repo['shizhan0_xt.gdpUS'].find()]

        cityEcon = []

        for (year, value) in gdpUS:
            if(year != "2018"):
                bpcpi = [x for x in pcpi if x[0] == year][0][1]
            if (year == "2016" or year == "2017"):
                s = [x for x in score if x[0] == year][0][1]
            else:
                s = "NA"
            if(value != "NA" and bpcpi != "NA"):
                e = float(value)/float(bpcpi)
            else:
                e = "NA"
            cityEcon.append({"Year": year, "AvgCityScore": s, "NationalGDP": value, "BostonPCPI": bpcpi, "Coefficient": e})

        repo.dropCollection("cityEcon")
        repo.createCollection("cityEcon")
        repo['shizhan0_xt.cityEcon'].insert_many(cityEcon)

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

        this_script = doc.agent('alg:#shizhan0_xt#cityEcon', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        pcpi = doc.entity('dat:shizhan0_xt#pcpi', {'prov:label': 'pcpi', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        cityScore = doc.entity('dat: dat:shizhan0_xt#cityScore', {'prov:label':'cityScore', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        gdpUS = doc.entity('dat: dat:shizhan0_xt#gdpUS', {'prov:label':'gdpUS', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_cityEcon = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_cityEcon, this_script)

        doc.usage(get_cityEcon, pcpi, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_cityEcon, cityScore, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_cityEcon, gdpUS, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

        cityEcon = doc.entity('dat:shizhan0_xt#cityEcon',
                              {prov.model.PROV_LABEL: 'cityEcon', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(cityEcon, this_script)
        doc.wasGeneratedBy(cityEcon, get_cityEcon, endTime)

        doc.wasDerivedFrom(cityEcon, pcpi, get_cityEcon, get_cityEcon, get_cityEcon)
        doc.wasDerivedFrom(cityEcon, cityScore, get_cityEcon, get_cityEcon, get_cityEcon)
        doc.wasDerivedFrom(cityEcon, gdpUS, get_cityEcon, get_cityEcon, get_cityEcon)

        repo.logout()

        return doc



# cityEcon.execute()
# doc = cityEcon.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
