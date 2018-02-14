import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class bikeComparisonCam(dml.Algorithm):
    contributor = 'cwsonn_levyjr'
    reads = ['cwsonn_levyjr.Cbikepath', 'cwsonn_levyjr.bikerack']
    writes = ['cwsonn_levyjr.bikeComparisonCam']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cwsonn_levyjr', 'cwsonn_levyjr')

        Cbikepath = repo['cwsonn_levyjr.Cbikepath'].find()

        #Get Cambridge bike path coordinates
        bikePathCoords = []
        for c in Cbikepath:
            pathCoords = c["geometry"]["coordinates"]
            bikePathCoords.append(pathCoords)
        print(bikePathCoords)

        #Get Cambridge bike rack locations
        bikeracks = repo['cwsonn_levyjr.bikerack'].find()
        bikeRackCoords = []
        for c in bikeracks:
            coords = c["geometry"]["coordinates"]
            bikeRackCoords.append(coords)
        print(bikeRackCoords)

        #Combine Data and Create new collection
        
        bike_lengths = []
        repo.dropCollection("cwsonn_levyjr.bikeComparisonCam")
        repo.createCollection("cwsonn_levyjr.bikeComparisonCam")

        bike_coord_dict = {'bikePathCoords': bikePathCoords, 'bikeRackCoords': bikeRackCoords}
        repo['cwsonn_levyjr.bikeComparisonCam'].insert_one(bike_coord_dict)
        
        combinedData = repo['cwsonn_levyjr.bikeComparisonCam'].find()
        
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
        repo.authenticate('cwsonn_levyjr', 'cwsonn_levyjr')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cwsonn_levyjr#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:cwsonn_levyjr#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:cwsonn_levyjr#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

bikeComparisonCam.execute()
doc = bikeComparisonCam.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

