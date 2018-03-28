import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'xhug'
    reads = []
    writes = ['xhug.hubways', 'xhug.bilkeavailability', 'xhug.Tstops', 'xhug.bostoncolleges', 'xhug.trafficejam']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xhug', 'xhug')

        url = 'http://datamechanics.io/data/xhug/hubways.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubways")
        repo.createCollection("hubways")
        repo['xhug.hubways'].insert_many(r)
        repo['xhug.hubways'].metadata({'complete':True})
        print(repo['xhug.hubways'].metadata())

        url = 'http://datamechanics.io/data/xhug/bilkeavailability.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bilkeavailability")
        repo.createCollection("bilkeavailability")
        repo['xhug.bilkeavailability'].insert_many(r)
        repo['xhug.bilkeavailability'].metadata({'complete':True})
        print(repo['xhug.bilkeavailability'].metadata())

        url = 'http://datamechanics.io/data/xhug/Tstops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Tstops")
        repo.createCollection("Tstops")
        repo['xhug.Tstops'].insert_many(r)
        repo['xhug.Tstops'].metadata({'complete':True})
        print(repo['xhug.Tstops'].metadata())

        url = 'http://datamechanics.io/data/xhug/trafficejam.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trafficejam")
        repo.createCollection("trafficejam")
        repo['xhug.trafficejam'].insert_many(r)
        repo['xhug.trafficejam'].metadata({'complete':True})
        print(repo['xhug.trafficejam'].metadata())

        url = 'http://datamechanics.io/data/xhug/bostoncolleges.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostoncolleges")
        repo.createCollection("bostoncolleges")
        repo['xhug.bostoncolleges'].insert_many(r)
        repo['xhug.bostoncolleges'].metadata({'complete':True})
        print(repo['xhug.bostoncolleges'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xhug', 'xhug')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
    
        doc.add_namespace('xda','https://datamechanics.io/data/xhug')

        this_script = doc.agent('alg:xhug#readdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_tstops = doc.entity('dio:Tstops', {'prov:label':'tstops locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_bikes = doc.entity('dio:bilkeavailability', {'prov:label':'bike availability', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_colleges = doc.entity('dio:bostoncolleges', {'prov:label':'colleges in boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_hubways = doc.entity('dio:hubways', {'prov:label':'hubways', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_jam = doc.entity('dio:trafficejam', {'prov:label':'traffic jams', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
    
        get_tstops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bikes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_colleges = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubways = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_jam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_tstops, this_script)
        doc.wasAssociatedWith(get_bikes, this_script)
        doc.wasAssociatedWith(get_colleges, this_script)
        doc.wasAssociatedWith(get_hubways, this_script)
        doc.wasAssociatedWith(get_jam, this_script)

        doc.usage(get_tstops, resource_tstops, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_bikes, resource_bikes, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_colleges, resource_colleges, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_hubways, resource_hubways, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_jam, resource_jam, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        tstops = doc.entity('dat:xhug#Tstops', {prov.model.PROV_LABEL:'tstops locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tstops, this_script)
        doc.wasGeneratedBy(tstops, get_tstops, endTime)
        doc.wasDerivedFrom(tstops, resource, get_tstops, get_tstops, get_tstops)

        bikes = doc.entity('dat:xhug#bikeavailability', {prov.model.PROV_LABEL:'bike availability', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikes, this_script)
        doc.wasGeneratedBy(bikes, get_bikes, endTime)
        doc.wasDerivedFrom(bikes, resource, get_bikes, get_bikes, get_bikes)

        colleges = doc.entity('dat:xhug#bostoncolleges', {prov.model.PROV_LABEL:'colleges in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(colleges, this_script)
        doc.wasGeneratedBy(colleges, get_colleges, endTime)
        doc.wasDerivedFrom(colleges, resource, get_colleges, get_colleges, get_colleges)

        repo.logout()
                  
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
