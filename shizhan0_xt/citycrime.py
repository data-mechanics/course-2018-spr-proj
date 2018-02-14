import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class citycrime(dml.Algorithm):
    contributor = 'shizhan0_xt'
    reads = ['shizhan0_xt.cityScore','shizhan0_xt.crime']
    writes = ['shizhan0_xt.citycrime']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('shizhan0_xt','shizhan0_xt')

        score = [(x['ETL_LOAD_DATE'][0:10],x['CTY_SCR_DAY_AGG']) for x in repo['shizhan0_xt.cityScore'].find()]
        crime = [x['OCCURRED_ON_DATE'][0:10] for x in repo['shizhan0_xt.crime'].find()]
        citycrime = []

        for date in score:
            s = date[1]
            c = crime.count(date[0])
            citycrime.append({"Date": date[0], "CityScore": s, "DailyCrimeCount": c, "Coefficient": int(c)/float(s)})
        #print(citycrime)

        repo.dropCollection("citycrime")
        repo.createCollection("citycrime")
        repo['shizhan0_xt.citycrime'].insert_many(citycrime)
        repo['shizhan0_xt.citycrime'].metadata({'complete': True})

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

        this_script = doc.agent('alg:#shizhan0_xt#citycrime', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crime = doc.entity('dat:shizhan0_xt#crime',
                           {'prov:label': 'crime', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        cityScore = doc.entity('dat: dat:shizhan0_xt#cityScore',
                               {'prov:label': 'cityScore', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        get_citycrime = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_citycrime, this_script)

        doc.usage(get_citycrime, crime, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_citycrime, cityScore, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

        citycrime = doc.entity('dat:shizhan0_xt#citycrime',
                               {prov.model.PROV_LABEL: 'citycrime', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(citycrime, this_script)
        doc.wasGeneratedBy(citycrime, get_citycrime, endTime)

        doc.wasDerivedFrom(citycrime, crime, get_citycrime, get_citycrime, get_citycrime)
        doc.wasDerivedFrom(citycrime, cityScore, get_citycrime, get_citycrime, get_citycrime)

        repo.logout()

        return doc



#citycrime.execute()
#doc = citycrime.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))