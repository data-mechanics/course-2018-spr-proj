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

class getGrads(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getGrads'] #CHANGE

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://datamechanics.io/data/rmak_rsc3/Colleges_and_Universities.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getGrads") 
        repo.createCollection("getGrads")
        repo['rmak_rsc3.getGrads'].insert(r)
    
        repo['rmak_rsc3.getGrads'].metadata({'complete':True})

        

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

        this_script = doc.agent('alg:rmak_rsc3#getGrads', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dm:Colleges_and_Universities', {'prov:label':'grads', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        getGrads = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getGrads, this_script)

        doc.usage(getGrads, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        grad = doc.entity('dat:rmak_rsc3#grads', {prov.model.PROV_LABEL:'grad', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(grad, this_script)
        doc.wasGeneratedBy(grad, getGrads, endTime)
        doc.wasDerivedFrom(grad, resource, getGrads, getGrads, getGrads)
        
        

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
