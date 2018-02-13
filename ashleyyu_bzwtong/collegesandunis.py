import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class collegesandunis(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong', 'ashleyyu_bzwtong']

    @staticmethod
    def execute(trial = False):
        '''Colleges and universities within the City of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        schools_json = json.loads(response)
        repo.dropCollection("collegesanduni")
        repo.createCollection("collegesanduni")
        repo['ashleyyu_bzwtong'].insert_many(schools_json)
        repo['ashleyyu_bzwtong'].metadata({'complete':True})
        print(repo['ashleyyu_bzwtong'].metadata())

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
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ashleyyu_bzwtong#signals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school, this_script)
        doc.usage(get_school, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=colleges+and+unis&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        schools = doc.entity('dat:ashleyyu_bzwtong#signals', {prov.model.PROV_LABEL:'Colleges and Unis', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_school, endTime)
        doc.wasDerivedFrom(schools, resource, get_school, get_school, get_school)

        repo.logout()
                  
        return doc

collegesandunis.execute()
doc = collegesandunis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
