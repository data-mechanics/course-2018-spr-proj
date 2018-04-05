import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
import csv
import prov
from pymongo import MongoClient   #change this 
import json
import scipy
import dml

import urllib.request
import prov.model
import datetime
import uuid

from scipy.stats import linregress

class stat_analysis(dml.Algorithm):
    contributor = 'bm181354_rikenm'
    reads = ['bm181354_rikenm.solutionLeastPopularStationsdb']
    writes = ['bm181354_rikenm.stat_analysis']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
        station_cursor = repo['bm181354_rikenm.solutionLeastPopularStationsdb']
    
    #y_label is obtained from our algorithm.
    #x_value contains column lat_normalized, lon_normalized and popularity_normalized of a bike station
    
    
        X = pd.DataFrame(list(station_cursor.find()))
        X_value = X.iloc[:,0:3]
        
        y_labels = X.iloc[:,3]
        
        
        df = pd.DataFrame(X_value, columns=['lat_normalized',"long_normalized","pop_normalized"])
        
        df.corr(method='pearson')  #pearson correlation table between variables in X.
        
        #relation between lat and our y label
        lat = linregress(X_value.iloc[:,0],y_labels)
        
        #relation between lon and our y label
        long = linregress(X_value.iloc[:,1],y_labels)
    
        #relation between popularity and our y label
        pop = linregress(X_value.iloc[:,2],y_labels)
        
        new_df = pd.DataFrame(
                              {'Lat': lat,
                              'long': long,
                              'pop':pop 
                              })
        
        r = json.loads(new_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)
        
        
        # clear
        repo.dropPermanent('stat_analysis')
        #repo.create_collection("trail_index")
        repo.createPermanent('stat_analysis')
        repo['bm181354_rikenm.stat_analysis'].insert_many(r)
        
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
        
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
        doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/?prefix=bm181354_rikenm/')
        
        this_script = doc.agent('alg:bm181354_rikenm#stat_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # change this
        resource = doc.entity('bdp:Emergency_Medical_Service_EMS_Stations', {'prov:label':'dataset of medical service in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_analysis, this_script)
        
        #change this
        doc.usage(get_analysis, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        
        analysis = doc.entity('dat:bm181354_rikenm#stat_analysis', {prov.model.PROV_LABEL:'stat_analysis', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(analysis, this_script)
        doc.wasGeneratedBy(analysis, get_analysis, endTime)
        doc.wasDerivedFrom(analysis, resource, get_analysis, get_analysis, get_analysis)
        
        
        repo.logout()
        return doc
'''
def stat_analysis():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
    station_cursor = repo['bm181354_rikenm.solutionLeastPopularStationsdb']

    #y_label is obtained from our algorithm. 
    #x_value contains column lat_normalized, lon_normalized and popularity_normalized of a bike station 


    X = pd.DataFrame(list(station_cursor.find()))
    X_labels = X.iloc[:,0:3]

    y_labels = X.iloc[:,3]


    df = pd.DataFrame(X_value, columns=['lat_normalized',"long_normalized","pop_normalized"])

    df.corr(method='pearson')  #pearson correlation table between variables in X.

    #relation between lat and our y label
    linregress(X_value[:,0],y_labels)

    #relation between lon and our y label
    linregress(X_value[:,1],y_labels)


    #relation between popularity and our y label
    linregress(X_value[:,2],y_labels)
    repo.logout()
'''

# See readme for average Linkage Dendrograms analysis.













