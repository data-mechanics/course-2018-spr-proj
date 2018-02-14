import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson

class getObesityData(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.obesityData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get obesity data
        url = 'https://chronicdata.cdc.gov/resource/mxg7-989n.json?locationdesc=Massachusetts&$limit=1000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        # Store in DB
        repo.dropCollection("obesityData")
        repo.createCollection("obesityData")
        repo['janellc_rstiffel.obesityData'].insert_many(r)
        repo['janellc_rstiffel.obesityData'].metadata({'complete':True})
        print(repo['janellc_rstiffel.obesityData'].metadata())

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
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/janellc_rstiffel/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/janellc_rstiffel/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # Resource:
        doc.add_namespace('crd', 'https://chronicdata.cdc.gov/resource/')

        this_script = doc.agent('alg:janellc_rstiffel#getObesityData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('crd:mxg7-989n', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_obesityData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_obesityData, this_script)

        doc.usage(get_obesityData, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?locationdesc=Massachusetts&$limit=1000'
                  }
                  )

        obesityData = doc.entity('dat:janellc_rstiffel#obesityData', {prov.model.PROV_LABEL:'Obesity Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesityData, this_script)
        doc.wasGeneratedBy(obesityData, get_obesityData, endTime)
        doc.wasDerivedFrom(obesityData, resource, get_obesityData, get_obesityData, get_obesityData)

        repo.logout()
                  
        return doc

# getObesityData.execute()
# doc = getObesityData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
