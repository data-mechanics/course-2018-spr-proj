# Filename: TransformMBTADwellTimes.py
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

class TransformMBTADwellTimes(dml.Algorithm):
    contributor = "bemullen_dharmesh"
    reads = ["bemullen_dharmesh.mbta_red_dwells", "mbta_green_dwells"]
    writes = ["bemullen_dharmesh.mbta_red_dwells_monthly", "bemullen_dharmesh.mbta_green_dwells_monthly"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        subprocess.check_output('mongo repo -u bemullen_dharmesh -p\
            bemullen_dharmesh --authenticationDatabase "repo" transformMBTADwellTimes.js', shell=True)

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
        
        mbta_red_dwells = doc.entity('dat:bemullen_dharmesh#mbta_red_dwells',
            {prov.model.PROV_LABEL:'Dwell Intervals for the MBTA\'s Red Line', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_mbta_red_dwells = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Dwell Intervals for the MBTA\'s Red Line'})
        doc.wasAssociatedWith(get_mbta_red_dwells, this_script)
        doc.used(get_mbta_red_dwells, mbta_red_dwells, startTime)
        doc.wasAttributedTo(mbta_red_dwells, this_script)
        doc.wasGeneratedBy(mbta_red_dwells, get_mbta_red_dwells, endTime) 

        mbta_green_dwells = doc.entity('dat:bemullen_dharmesh#mbta_red_dwells',
            {prov.model.PROV_LABEL:'Dwell Intervals for the MBTA\'s Green Line', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_mbta_red_dwells = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Dwell Intervals for the MBTA\'s Green Line'})
        doc.wasAssociatedWith(get_mbta_green_dwells, this_script)
        doc.used(get_mbta_green_dwells, mbta_green_dwells, startTime)
        doc.wasAttributedTo(mbta_green_dwells, this_script)
        doc.wasGeneratedBy(mbta_green_dwells, get_mbta_green_dwells, endTime)        
        
        repo.logout()
        return doc