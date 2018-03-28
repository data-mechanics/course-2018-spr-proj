import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json

class getStreetlights(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = []
    writes = ['aoconno8_dmak1112_ferrys.streetlights']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')

        url = 'https://data.boston.gov/dataset/52b0fdad-4037-460c-9c92-290f5774ab2b/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5/download/streetlight-locations.csv'
        streetlight_dict = pd.read_csv(url).to_dict(orient='records')
                
        repo.dropCollection('streetlights')
        repo.createCollection('streetlights')
        repo['aoconno8_dmak1112_ferrys.streetlights'].insert_many(streetlight_dict)
        repo['aoconno8_dmak1112_ferrys.streetlights'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.streetlights'].metadata())

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
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getStreetlights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:streetlight-locations', {'prov:label':'Streetlight Location Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.usage(get_streetlights, resource, startTime, None,
                  {
                    prov.model.PROV_TYPE:'ont:Retrieval'
                  })


        streetlight_locations = doc.entity('dat:aoconno8_dmak1112_ferrys#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlight_locations, this_script)
        doc.wasGeneratedBy(streetlight_locations, get_streetlights, endTime)
        doc.wasDerivedFrom(streetlight_locations, resource, get_streetlights, get_streetlights, get_streetlights)

        repo.logout()
                  
        return doc

#getStreetlights.execute()
#doc = getStreetlights.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

