#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 14:34:57 2018

@author: rhondamak1
"""

import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getHubway(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getHubway'] 

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://datamechanics.io/data/rmak_rsc3/Hubway.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getHubway") 
        repo.createCollection("getHubway")
        repo['rmak_rsc3.getHubway'].insert_many(r)
    
        repo['rmak_rsc3.getHubway'].metadata({'complete':True})
        print(repo['rmak_rsc3.getHubway'].metadata())
        

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
        doc.add_namespace('hub', 'http://datamechanics.io/data/rmak_rsc3/')

        this_script = doc.agent('alg:rmak_rsc3#getHubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('hub:Hubway', {'prov:label':'HubStations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getHubs = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getHubs, this_script)

        doc.usage(getHubs, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        hub = doc.entity('dat:rmak_rsc3#hubstations', {prov.model.PROV_LABEL:'hubstations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hub, this_script)
        doc.wasGeneratedBy(hub, getHubs, endTime)
        doc.wasDerivedFrom(hub, resource, getHubs, getHubs, getHubs)
        
        

        repo.logout()
                  
        return doc
    
#print(getHubway.execute())
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
