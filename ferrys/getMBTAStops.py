import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getMBTAStops(dml.Algorithm):
    contributor = 'ferrys'
    reads = []
    writes = ['ferrys.mbta']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')

        api_key = dml.auth['services']['mbtadeveloperportal']['key']
    
        url = 'https://api-v3.mbta.com/stops?api_key='+ api_key
        response = urllib.request.urlopen(url).read().decode("utf-8")
        mbta_json = json.loads(response)['data']
                
        repo.dropCollection('mbta')
        repo.createCollection('mbta')
        repo['ferrys.mbta'].insert_many(mbta_json)
        repo['ferrys.mbta'].metadata({'complete':True})
        print(repo['ferrys.mbta'].metadata())

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
        repo.authenticate('ferrys', 'ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mbta', 'https://api-v3.mbta.com/stops')

        this_script = doc.agent('alg:ferrys#getMBTAStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbta:stops', {'prov:label':'MBTA Developer Portal', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta_stops, this_script)
        doc.usage(get_mbta_stops, resource, startTime, None,
                  {
                    prov.model.PROV_TYPE:'ont:Retrieval'
                  })


        mbta_stops= doc.entity('dat:ferrys#mbta', {prov.model.PROV_LABEL:'mbta', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
        doc.wasDerivedFrom(mbta_stops, resource, get_mbta_stops, get_mbta_stops, get_mbta_stops)

        repo.logout()
                  
        return doc

#getMBTAStops.execute()
#doc = getMBTAStops.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

