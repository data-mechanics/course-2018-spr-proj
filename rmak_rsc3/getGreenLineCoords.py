#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 11:10:18 2018

@author: Rachel
"""

import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getGreenLineCoords(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getGreenLineCoords'] 

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://datamechanics.io/data/rmak_rsc3/GreenLine.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getGreenLineCoords") 
        repo.createCollection("getGreenLineCoords")
        repo['rmak_rsc3.getGreenLineCoords'].insert_many(r)
    
        repo['rmak_rsc3.getGreenLineCoords'].metadata({'complete':True})
        print(repo['rmak_rsc3.getGreenLineCoords'].metadata())
        

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

        this_script = doc.agent('alg:rmak_rsc3#getGreenLineCoords', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dm:GreenLine', {'prov:label':'coords', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getGLC = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getGLC, this_script)

        doc.usage(getGLC, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        Reliability = doc.entity('dat:rmak_rsc3#coords', {prov.model.PROV_LABEL:'Reliability', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Reliability, this_script)
        doc.wasGeneratedBy(Reliability, getGLC, endTime)
        doc.wasDerivedFrom(Reliability, resource, getGLC, getGLC, getGLC)
        
        

        repo.logout()
                  
        return doc
    
#print(getGreenLineCoords.execute())
'''
getUniversities.execute()
print('doc = get_fireHydrant.provenance()')
doc = getUniversities.provenance()
print('doc.get_provn()')
print(doc.get_provn())
print('json.dumps(json.loads(doc.serialize()), indent=4')
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof