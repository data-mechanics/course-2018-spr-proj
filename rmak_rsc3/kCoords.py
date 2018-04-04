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
from numpy import random
import math 
from sklearn.cluster import KMeans

class kCoords(dml.Algorithm):

    contributor = 'rmak_rsc3'
    reads = ['rmak_rsc3.getReliability','rmak_rsc3.getGreenLineCoords','rmak_rsc3.getHubway' ]
    writes = ['rmak_rsc3.kCoords'] #CHANGE

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        
        
        
        stations = list(repo['rmak_rsc3.getReliability'].find({}))
        
        coords=list(repo['rmak_rsc3.getGreenLineCoords'].find({}))
        
        hubway = list(repo['rmak_rsc3.getHubway'].find({})) 
        
        if trial:
            stations = stations[0:20]
            hubway = hubway[0:10]
            
          
                
        lateTrains = {}
        for s in stations:   
            lateTrains[s['STOP']] = lateTrains.get(s['STOP'], 0) + int(round(s['OTP_NUMERATOR'],0))
            
        lateTrainCoords = []
        

        
        for key in lateTrains:
            for c in coords:
                if key in c['Station Name'] or c['Station Name'] in key:
                    lateTrainCoords += [[c['Latitude'],c['Longitude']]] *lateTrains[key]
        
        

        hubwayCoords = []
        for i in hubway:
            hubwayCoords += [(i['Latitude'],i['Longitude'])] 



#lateTrainCoords = [i[1] for i in lateTrainCoords]
#print(hubwayCoords)


#        M = hubwayCoords
#        print('assigned M')
        random.shuffle(lateTrainCoords)
 
        P = lateTrainCoords
        

        kmeans = KMeans(n_clusters= 40, random_state=0).fit(P)



        
        clusterInsert = {}
        for i in range(len(kmeans.cluster_centers_)):

            i = str(i)
            clusterInsert[i] = kmeans.cluster_centers_.tolist()[int(i)]

            

        
        repo.dropCollection("kCoords") 
        repo.createCollection("kCoords")
        repo['rmak_rsc3.kCoords'].insert(clusterInsert)
    
        repo['rmak_rsc3.kCoords'].metadata({'complete':True})

        

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
        
        # Set up the database connection.
#        client = dml.pymongo.MongoClient()
#        repo = client.repo
#        repo.authenticate('rmak_rsc3', 'rmak_rsc3')
#        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#        doc.add_namespace('dm', 'http://datamechanics.io/data/rmak_rsc3/')
#        doc.add_namespace('hub', 'http://datamechanics.io/data/rmak_rsc3/')
#        doc.add_namespace('dm', 'http://datamechanics.io/data/rmak_rsc3/')
#
#        this_script = doc.agent('alg:rmak_rsc3#kCoords', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#        greenline_dataset = doc.entity('dm:GreenLine', {'prov:label':'coords', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#        hubway_dataset = doc.entity('hub:Hubway', {'prov:label':'HubStations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#        reliability_dataset = doc.entity('dm:GreenLine', {'prov:label':'coords', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#
#    
#        get_kCoords = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#
#        doc.wasAssociatedWith(get_kCoords, this_script)
#
#        #Query might need to be changed
#        doc.usage(get_kCoords,greenline_dataset, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval'
#                 
#                  }
#                  )
#        doc.usage(get_kCoords, hubway_dataset, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval'
#                  
#                  }
#                  )
#        
#        doc.usage(get_kCoords, reliability_dataset, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval'
#                  
#                  }
#                  )
#
#
#        k_coords = doc.entity('dat:rmak_rsc3#kCoords', {'prov:label':'kmeans coords', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#        
#        doc.wasAttributedTo(k_coords, this_script)
#        doc.wasGeneratedBy(k_coords, get_kCoords, endTime)
##        doc.wasDerivedFrom(k_coords, greenline_dataset, get_kCoords, get_kCoords, get_kCoords)
##        doc.wasDerivedFrom(k_coords, hubway_dataset, get_kCoords, get_kCoords, get_kCoords)
##        doc.wasDerivedFrom(k_coords, reliability_dataset, get_kCoords, get_kCoords, get_kCoords)
#
#        repo.logout()
#                  
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
