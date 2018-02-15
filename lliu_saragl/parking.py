import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class parking(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = []
    writes = ['lliu_saragl.parking']

    @staticmethod
    def execute(trial = False):
        '''Retrieve data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/53ebc23fcc654111b642f70e61c63852_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #print(r)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)
        repo.dropPermanent("parking")
        repo.createPermanent("parking")
        repo['lliu_saragl.parking'].insert_many(r['features'])
        repo['lliu_saragl.violations'].metadata({'complete':True})
        print(repo['lliu_saragl.violations'].metadata())

        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening in this script.
        Each run of the script will generate a new document describing that invocation event.
        '''

        # Set up the database connection 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bos', 'http://bostonopendata.boston.opendata.arcgis.com/') # Parking data

        this_script = doc.agent('alg:lliu_saragl#parking', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bos:53ebc23fcc654111b642f70e61c63852_0', {'prov:label':'Snow Emergency Parking', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        get_parking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_parking, this_script)
        doc.usage(get_parking, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        park = doc.entity('dat:lliu_saragl#parking',{prov.model.PROV_LABEL:'Snow Emergency Parking', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(park, this_script)
        doc.wasGeneratedBy(park, get_parking, endTime)
        doc.wasDerivedFrom(park, get_parking, get_parking, get_parking)

        
        repo.logout()

        return doc

#parking.execute()
#doc = parking.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
