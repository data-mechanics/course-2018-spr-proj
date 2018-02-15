# Filename: TransformCityScores.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import subprocess
import xmltodict

class TransformCityScores(dml.Algorithm):
    contributor = "bemullen_dharmesh"
    reads = ["bemullen_dharmesh.cityscores"]
    writes = ["bemullen_dharmesh.cityscores_monthly"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        subprocess.check_output('mongo repo -u bemullen_dharmesh -p\
            bemullen_dharmesh --authenticationDatabase "repo" transformCityScores.js', shell=True)

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:bemullen_dharmesh#TransformCityScores', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        cityscores = doc.entity('dat:bemullen_dharmesh#cityscores', {prov.model.PROV_LABEL:'CityScores Boston', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_cityscores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'A consolidated metric measuring Boston\'s residents\' satisfaction'})        
        doc.wasAssociatedWith(get_cityscores, this_script)
        doc.used(get_cityscores, cityscores, startTime)
        doc.wasAttributedTo(cityscores, this_script)
        doc.wasGeneratedBy(cityscores, get_cityscores, endTime)        
        
        repo.logout()
        return doc

TransformCityScores.execute()