import dml
import prov.model
import uuid
import datetime
import json

class parking_routes(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = ['lliu_saragl.parking', 'lliu_saragl.routes']
    writes = ['lliu_saragl.parking_routes']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        repo.dropPermanent("parking_routes")
        repo.createPermanent("parking_routes")
        
        def getData(db):
            d = []
            for i in repo['lliu_saragl.' + db].find():
                d.append(i)
            return d

        p = getData('parking')

        r = getData('routes')

        lstOfParking = {'result':[]}
        for i in p:
            if i['properties']['Address'] not in lstOfParking:
                lstOfParking['result'].append(i['properties']['Address'])
                

        lstOfRoutes = {'result':[]}
        for i in r:
            if i['properties']['FULL_NAME'] not in lstOfRoutes:
                lstOfRoutes['result'].append(i['properties']['FULL_NAME'])

        u = {**lstOfParking, **lstOfRoutes}

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

        this_script = doc.agent('alg:lliu_saragl#parking_routes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:lliu_saragl#parking', {prov.model.PROV_LABEL:'Parking Resource', prov.model.PROV_TYPE:'ont:DataSet'})

        get_parkingroute = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAttributedTo(resource, this_script)
        doc.wasGeneratedBy(resource, get_parkingroute, endTime)
        doc.wasDerivedFrom(resource, get_parkingroute, get_parkingroute, get_parkingroute)

        repo.logout()
        return doc

parking_routes.execute()
doc = parking_routes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
