import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np


class openSpaceComparison(dml.Algorithm):
    contributor = 'cwsonn_levyjr'
    reads = ['cwsonn_levyjr.Copenspace', 'cwsonn_levyjr.openspace']
    writes = ['cwsonn_levyjr.openSpaceComparison']
    
    def intersect(R, S):
        return [t for t in R if t in S]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cwsonn_levyjr', 'cwsonn_levyjr')

        BopenSpace = repo['cwsonn_levyjr.openspace'].find()

        #Get Boston open space areas
        parkAreasBos = []
        for b in BopenSpace:
            area = b["properties"]["ShapeSTArea"]
            parkAreasBos.append(area)

        CopenSpace = repo['cwsonn_levyjr.Copenspace'].find()

        #Get Cambridge open space areas
        CamAreaTotal = 0
        parkAreasCam = []
        for c in CopenSpace:
            area = c["shape_area"]
            parkAreasCam.append(area)

        #Combine Data
        repo.dropCollection("cwsonn_levyjr.openSpaceComparison")
        repo.createCollection("cwsonn_levyjr.openSpaceComparison")

        park_dict = {'BosOpenSpaces': parkAreasBos, 'CamOpenSpaces': parkAreasCam}
        repo['cwsonn_levyjr.openSpaceComparison'].insert_one(park_dict)
        
        combinedData = repo['cwsonn_levyjr.openSpaceComparison'].find()

        BosAreaTotal = 0
        CamAreaTotal = 0
        for b in combinedData:
            areaB = b["BosOpenSpaces"]
            areaC = b["CamOpenSpaces"]
            x = np.array(areaC)
            areaC = x.astype(np.float)
            
            for i in range(len(areaB)):
                BosAreaTotal += areaB[i]
            for j in range(len(areaC)):
                CamAreaTotal += areaC[j]
    
        areaCount = 0
        for i in range(len(areaB)):
            for j in range(len(areaC)):
                if(areaC[j] - 10 <= areaB[i] <= areaC[j] + 10):
                    areaCount += 1

        bikeAreaTotals = {'BosOpenSpaceAreaTotal': BosAreaTotal, 'CamOpenSpaceAreaTotal': CamAreaTotal}
        bikeIntersectionsTotals = {'IntersectionTotals': areaCount}

        repo['cwsonn_levyjr.openSpaceComparison'].insert_one(bikeAreaTotals)
        repo['cwsonn_levyjr.openSpaceComparison'].insert_one(bikeIntersectionsTotals)


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
        doc.add_namespace('bod', 'https://data.cambridgema.gov/Planning/Open-Space/q73m-a5e2')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
    
        #Agent
        this_script = doc.agent('alg:openSpaceComparison', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Resources
        openspace = doc.entity('dat:openspace', {'prov:label': 'BopenSpace', prov.model.PROV_TYPE:'ont:DataResource'})
        Copenspace = doc.entity('dat:Copenspace', {'prov:label': 'CopenSpace', prov.model.PROV_TYPE:'ont:DataResource'})
        
        #Activities
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        
        #Usage
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, openspace, startTime)
        doc.used(this_run, Copenspace, startTime)
        
        # New dataset
        
        openSpaceComparison = doc.entity('dat:openSpaceComparison', {prov.model.PROV_LABEL:'openSpaceComparison',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openSpaceComparison, this_script)
        doc.wasGeneratedBy(openSpaceComparison, this_run, endTime)
        doc.wasDerivedFrom(openSpaceComparison, openspace, this_run, this_run, this_run)
        doc.wasDerivedFrom(openSpaceComparison, Copenspace, this_run, this_run, this_run)
        
        
        repo.logout()

        return doc

'''openSpaceComparison.execute()
doc = openSpaceComparison.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))'''

