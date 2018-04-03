import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class bikeComparison(dml.Algorithm):
    contributor = 'cwsonn_levyjr'
    reads = ['cwsonn_levyjr.Cbikepath', 'cwsonn_levyjr.bikepath']
    writes = ['cwsonn_levyjr.bikeLengthComparison']
    
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

        Bbikepath = repo['cwsonn_levyjr.bikepath'].find()

        #Get Boston bike path lengths
        bikeLenBos = []
        for b in Bbikepath:
            len = b["properties"]["Shape_Leng"]
            bikeLenBos.append(len)
        
        BosBikeTotal = 0
        for i in bikeLenBos:
            BosBikeTotal += i

        Cbikepath = repo['cwsonn_levyjr.Cbikepath'].find()

        #Get Cambridge bike path lengths
        bikeLenCam = []
        for c in Cbikepath:
            len = c["properties"]["LENGTH"]
            bikeLenCam.append(len)

        CamBikeTotal = 0
        for i in bikeLenCam:
            CamBikeTotal += i
        
        bikeCompTotals = {'BosBikePathLengthTotal': BosBikeTotal, 'CamBikePathLengthTotal': CamBikeTotal}

        #Combine Data and Create new collection
        bike_lengths = []
        repo.dropCollection("cwsonn_levyjr.bikeLengthComparison")
        repo.createCollection("cwsonn_levyjr.bikeLengthComparison")

        bike_length_dict = {'BosBikePath': bikeLenBos, 'CamBikePath': bikeLenCam, 'pathTotals': bikeCompTotals}
        repo['cwsonn_levyjr.bikeLengthComparison'].insert_one(bike_length_dict)
        
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
        this_script = doc.agent('alg:bikeComparison', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Resources
        bikepath = doc.entity('dat:bikepath', {'prov:label': 'bikepath', prov.model.PROV_TYPE:'ont:DataResource'})
        Cbikepath = doc.entity('dat:Cbikepath', {'prov:label': 'Cbikepath', prov.model.PROV_TYPE:'ont:DataResource'})
        
        #Activities
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        
        #Usage
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, bikepath, startTime)
        doc.used(this_run, Cbikepath, startTime)
        
        # New dataset
        
        bikeComparison = doc.entity('dat:bikeLengthComparison', {prov.model.PROV_LABEL:'bikeLengthComparison',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeComparison, this_script)
        doc.wasGeneratedBy(bikeComparison, this_run, endTime)
        doc.wasDerivedFrom(bikeComparison, bikepath, this_run, this_run, this_run)
        doc.wasDerivedFrom(bikeComparison, Cbikepath, this_run, this_run, this_run)
        
        repo.logout()
        
        return doc

'''bikeComparison.execute()
doc = bikeComparison.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4)'''

