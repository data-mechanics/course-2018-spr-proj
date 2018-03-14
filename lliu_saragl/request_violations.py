import dml
import prov.model
import uuid
import datetime
import json

class request_violations(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = ['lliu_saragl.requests', 'lliu_saragl.violations']
    writes = ['lliu_saragl.request_violations']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        repo.dropPermanent("request_violations")
        repo.createPermanent("request_violations")
        
        def getData(db):
            d = []
            for i in repo['lliu_saragl.' + db].find():
                d.append(i)
            return d

        p = getData('requests')

        r = getData('violations')

        lstOfRequests = {}
        for i in p:
            l = i['LOCATION_ZIPCODE']
            if l not in lstOfRequests:
                l = l.replace(".","")
                lstOfRequests[l] = {"Type":i['TYPE']}

        lstOfViolations = {}
        for i in r:
            temp = i['Zip']
            if temp not in lstOfViolations:
                temp = temp.replace(".","")
                lstOfViolations[temp] = {}

        
        u = {**lstOfRequests, **lstOfViolations}
        
        print(u)
        
        repo['lliu_saragl.parking_routes'].insert_one(u)
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        doc = prov.model.ProvDocument()
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bos', 'http://bostonopendata.boston.opendata.arcgis.com/') # Parking data

        this_script = doc.agent('alg:lliu_saragl#request_violations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:lliu_saragl#requests', {prov.model.PROV_LABEL:'311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})

        get_requestviolations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAttributedTo(resource, this_script)
        doc.wasGeneratedBy(resource, get_requestviolations, endTime)
        doc.wasDerivedFrom(resource, get_requestviolations, get_requestviolations, get_requestviolations)

        repo.logout()
        return doc

request_violations.execute()
doc = request_violations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
