#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 21:33:54 2018

@author: rhondamak1
"""


import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getSites(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getSites'] #CHANGE

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://datamechanics.io/data/rmak_rsc3/servicereq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getSites") 
        repo.createCollection("getSites")
        repo['rmak_rsc3.getSites'].insert_many(r)
    
        repo['rmak_rsc3.getSites'].metadata({'complete':True})
        print(repo['rmak_rsc3.getSites'].metadata())
        

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
        repo.authenticate('rmak_rsc3', 'rmak_rsc3')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dm', 'http://datamechanics.io/data/rmak_rsc3/')

        this_script = doc.agent('alg:rmak_rsc3#getSites', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dm:servicereq', {'prov:label':'sites', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getSites = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getSites, this_script)

        doc.usage(getSites, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        sites = doc.entity('dat:rmak_rsc3#sites', {prov.model.PROV_LABEL:'site', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sites, this_script)
        doc.wasGeneratedBy(sites, getSites, endTime)
        doc.wasDerivedFrom(sites, resource, getSites, getSites, getSites)
        
        

        repo.logout()
                  
        return doc
    '''
print('get_fireHydrant.execute()')
getUniversities.execute()
print('doc = get_fireHydrant.provenance()')
doc = getUniversities.provenance()
print('doc.get_provn()')
print(doc.get_provn())
print('json.dumps(json.loads(doc.serialize()), indent=4')
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
