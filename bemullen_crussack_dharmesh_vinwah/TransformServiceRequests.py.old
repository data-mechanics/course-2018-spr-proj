# Filename: TransformServiceRequests.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
#
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

class TransformServiceRequests(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = ["bemullen_crussack_dharmesh_vinwah.service_requests"]
    writes = ["bemullen_crussack_dharmesh_vinwah.service_requests_monthly"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        subprocess.check_output('mongo repo -u bemullen_crussack_dharmesh_vinwah -p\
            bemullen_crussack_dharmesh_vinwah --authenticationDatabase "repo" transformServiceRequests.js', shell=True)

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#TransformServiceRequests', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        service_requests = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#service_requests', {prov.model.PROV_LABEL:'311 Service Requests', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_service_requests = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Boston City\'s 311 Service Requests Portal'})        
        doc.wasAssociatedWith(get_service_requests, this_script)
        doc.used(get_service_requests, service_requests, startTime)
        doc.wasAttributedTo(service_requests, this_script)
        doc.wasGeneratedBy(service_requests, get_service_requests, endTime)        
        
        repo.logout()
        return doc

