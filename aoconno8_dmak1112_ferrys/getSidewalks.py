import urllib.request
import geojson
import dml
import prov.model
import datetime
import uuid
import json

class getSidewalks(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = []
    writes = ['aoconno8_dmak1112_ferrys.sidewalks']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/6aa3bdc3ff5443a98d506812825c250a_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        sidewalk_dict = dict(geojson.loads(response))['features']
                
        repo.dropCollection("sidewalks")
        repo.createCollection("sidewalks")
        repo['aoconno8_dmak1112_ferrys.sidewalks'].insert_many(sidewalk_dict)
        repo['aoconno8_dmak1112_ferrys.sidewalks'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.sidewalks'].metadata())

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
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getSidewalks', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:6aa3bdc3ff5443a98d506812825c250a_0', {'prov:label':'Sidewalk Location Geo Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_sidewalks = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sidewalks, this_script)
        doc.usage(get_sidewalks, resource, startTime, None,
                  {
                    prov.model.PROV_TYPE:'ont:Retrieval'
                  })


        sidewalk_locations = doc.entity('dat:aoconno8_dmak1112_ferrys#sidewalks', {prov.model.PROV_LABEL:'sidewalks', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sidewalk_locations, this_script)
        doc.wasGeneratedBy(sidewalk_locations, get_sidewalks, endTime)
        doc.wasDerivedFrom(sidewalk_locations, resource, get_sidewalks, get_sidewalks, get_sidewalks)

        repo.logout()
                  
        return doc

#getSidewalks.execute()
#doc = getSidewalks.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

