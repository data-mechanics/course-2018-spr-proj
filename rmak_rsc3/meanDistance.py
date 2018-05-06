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
import math

def coordsToFeet(coord1, coord2):
    return 131332796.6 * (math.acos(math.cos(coord1[0]) * math.cos(coord1[1]) * math.cos(coord2[0]) 
    * math.cos(coord2[1]) + math.cos(coord1[0]) * math.sin(coord1[1]) * math.cos(coord2[0]) 
    * math.sin(coord2[1]) + math.sin(coord1[0]) * math.sin(coord2[0]))/360)
        
def meanDistances(coordList,hubwayList):
    testDistance = 0
    for i in coordList:
        newList = [0] * len(hubwayList)
        for x in range(len(hubwayList)):
            newList[x] = coordsToFeet(hubwayList[x], i)
            #sortedList = sorted(newList)
        testDistance += min(newList)

    return testDistance/len(coordList)
    
class meanDistance(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = ['rmak_rsc3.getHubway', 'rmak_rsc3.kCoords']
    writes = ['rmak_rsc3.meanDistance'] #CHANGE

    @staticmethod
    
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
                
     
        hubway = list(repo['rmak_rsc3.getHubway'].find({}))
        hubwayCoords = []

        for i in hubway:
            hubwayCoords += [(i['Latitude'],i['Longitude'])] 
        if trial:

            hubwayCoords = hubwayCoords[0:10]

        kCoords = list(repo['rmak_rsc3.kCoords'].find({}))

        lkCoords = []

        count = 0
        for i in kCoords[0]:
            if count == 0:
                count += 1
                continue
            lkCoords.append(kCoords[0][i])
            count += 1

        
        mean = meanDistances(lkCoords,hubwayCoords)


        
        repo.dropCollection("meanDistance") 
        repo.createCollection("meanDistance")
        repo['rmak_rsc3.meanDistance'].insert({'MEAN':mean})
    
        repo['rmak_rsc3.meanDistance'].metadata({'complete':True})

        

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
   
      
    @staticmethod

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        
#         Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dm', 'http://datamechanics.io/data/rmak_rsc3/')
        doc.add_namespace('hub', 'http://datamechanics.io/data/rmak_rsc3/')

        this_script = doc.agent('alg:rmak_rsc3#meanDistance', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        getHubway_resource = doc.entity('hub:Hubway', {'prov:label':'HubStations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        getmeanDistance = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getmeanDistance, this_script)

        doc.usage(getmeanDistance, getHubway_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        meanDist = doc.entity('dat:rmak_rsc3#mean', {prov.model.PROV_LABEL:'meanDist', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(meanDist, this_script)
        doc.wasGeneratedBy(meanDist, getmeanDistance, endTime)
        doc.wasDerivedFrom(meanDist, getHubway_resource, getmeanDistance, getmeanDistance, getmeanDistance)
        
        

        repo.logout()
                  
        return doc
#        '''
#    '''
#print('get_fireHydrant.execute()')
#getUniversities.execute()
#print('doc = get_fireHydrant.provenance()')
#doc = getUniversities.provenance()
#print('doc.get_provn()')
#print(doc.get_provn())
#print('json.dumps(json.loads(doc.serialize()), indent=4')
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#'''
## eof
