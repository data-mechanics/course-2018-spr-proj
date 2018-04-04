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
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = ["bemullen_crussack_dharmesh_vinwah.cityscores"]
    writes = ["bemullen_crussack_dharmesh_vinwah.cityscores_monthly"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        subprocess.check_output(('mongo repo -u bemullen_crussack_dharmesh_vinwah -p '
            'bemullen_crussack_dharmesh_vinwah --authenticationDatabase "repo" '
            'bemullen_crussack_dharmesh_vinwah/transformCityScores.js'), shell=True)

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')


        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#TransformCityScores',\
            {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        cityscores = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#cityscores',\
            {prov.model.PROV_LABEL:'CityScores Boston', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_cityscores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'A consolidated metric measuring Boston\'s residents\' satisfaction'})        
        doc.wasAssociatedWith(get_cityscores, this_script)
        doc.used(get_cityscores, cityscores, startTime)
        doc.wasAttributedTo(cityscores, this_script)
        doc.wasGeneratedBy(cityscores, get_cityscores, endTime)        
        
        repo.logout()
        return doc