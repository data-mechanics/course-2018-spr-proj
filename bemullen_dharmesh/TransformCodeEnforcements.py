# Filename: TransformCodeEnforcements.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
#
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import subprocess
import xmltodict

class TransformCodeEnforcements(dml.Algorithm):
    contributor = "bemullen_dharmesh"
    reads = ["bemullen_dharmesh.code_enforcements"]
    writes = ["bemullen_dharmesh.enforcements_monthly"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        subprocess.check_output('mongo repo -u bemullen_dharmesh -p\
            bemullen_dharmesh --authenticationDatabase "repo" transformCodeEnforcements.js', shell=True)

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


        this_script = doc.agent('alg:bemullen_dharmesh#TransformCodeEnforcements', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        code_enforcements = doc.entity('dat:bemullen_dharmesh#code_enforcements', {prov.model.PROV_LABEL:'Code Enforcement City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_code_enforcements = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Property and Code Violations'})        
        doc.wasAssociatedWith(get_code_enforcements, this_script)
        doc.used(get_code_enforcements, code_enforcements, startTime)
        doc.wasAttributedTo(code_enforcements, this_script)
        doc.wasGeneratedBy(code_enforcements, get_code_enforcements, endTime)        
        
        repo.logout()
        return doc
