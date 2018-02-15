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
        lenlst = []
        for c in Cbikepath:
            pathCoords = c["geometry"]["coordinates"]
            len = c["properties"]["LENGTH"]
            bikePathCoords.append(pathCoords)
            lenlst.append(len)
        
        #Get Cambridge bike rack locations
        bikeracks = repo['cwsonn_levyjr.bikerack'].find()
        
        bikeRackCoords = []
        rackTotal = 0
        for c in bikeracks:
            coords = c["geometry"]["coordinates"]
            rackTotal += 1
            bikeRackCoords.append(coords)
        
        racksPerPaths = sum(lenlst) / rackTotal

        #Combine Data and Create new collection
        repo.dropCollection("cwsonn_levyjr.bikeComparisonCam")
        repo.createCollection("cwsonn_levyjr.bikeComparisonCam")

        bike_coord_dict = {'bikePathCoords': bikePathCoords, 'bikeRackCoords': bikeRackCoords, 'numRacksPerPaths': racksPerPaths}
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log#')
        doc.add_namespace('bod', 'https://github.com/cambridgegis/cambridgegis_data/blob/master/Recreation/Bike_Facilities/RECREATION_BikeFacilities.geojson')
        doc.add_namespace('bod', 'https://github.com/cambridgegis/cambridgegis_data/blob/master/Recreation/Bike_Racks/RECREATION_BikeRacks.geojson')
        
        #Agent
        this_script = doc.agent('alg:bikeComparisonCam', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Resources
        bikerack = doc.entity('dat:bikerack', {'prov:label': 'bikerack', prov.model.PROV_TYPE:'ont:DataResource'})
        Cbikepath = doc.entity('dat:Cbikepath', {'prov:label': 'Cbikepath', prov.model.PROV_TYPE:'ont:DataResource'})
        
        #Activities
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        
        #Usage
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, bikerack, startTime)
        doc.used(this_run, Cbikepath, startTime)
        
        # New dataset
        
        bikeComparisonCam = doc.entity('dat:bikeComparisonCam', {prov.model.PROV_LABEL:'bikeComparisonCam',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeComparisonCam, this_script)
        doc.wasGeneratedBy(bikeComparisonCam, this_run, endTime)
        doc.wasDerivedFrom(bikeComparisonCam, bikerack, this_run, this_run, this_run)
        doc.wasDerivedFrom(bikeComparisonCam, Cbikepath, this_run, this_run, this_run)
        
        
        repo.logout()
        
        return doc
'''
bikeComparisonCam.execute()
doc = bikeComparisonCam.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

