import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import csv

from math import sin, cos, sqrt, atan2, radians
from scipy.spatial.distance import pdist
import math
from collections import Counter

from pymongo import MongoClient
import json
from sklearn.cluster import KMeans

import urllib.request


import dml
import prov.model
import datetime
import uuid


class solutionCluster(dml.Algorithm):
    contributor = 'bm181354_rikenm'
    reads = ['bm181354_rikenm.hubwayJuly','bm181354_rikenm.hubwayOne','bm181354_rikenm.hubwayTwo','bm181354_rikenm.hubwayThree']
    writes = ['bm181354_rikenm.solutionClusteringdb']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')


        station_cursor = repo['bm181354_rikenm.hubwayJuly']


        trip_data1= repo['bm181354_rikenm.hubwayOne']

        trip_data2= repo['bm181354_rikenm.hubwayTwo']

        trip_data3= repo['bm181354_rikenm.hubwayThree']


        #clustering based on distance
        def lat_dist(x,y):
              
            R = 6373.0

            lat1 =  radians(abs(x[0]))
            lon1 =  radians(abs(x[1]))
            lat2 =  radians(abs(y[0]))
            lon2 =  radians(abs(y[1]))

            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))

            distance = R * c


            return distance

        #Pairwise distances between observations in n-dimensional space aka "pdist".
        def geo_affinity(M):
            return  np.array([[lat_dist(a,b) for a in M] for b in M])


        # read data

        data = pd.DataFrame(list(station_cursor.find()))


        if iter == True:
            times = 50
            data = data.iloc[:50,:]

        else:
            times = 193
            
            
        # clustering based on popularity and distance
        #read 

        data2 = pd.DataFrame(list(trip_data1.find()))

        data3 = pd.DataFrame(list(trip_data2.find()))

        data4 = pd.DataFrame(list(trip_data3.find()))

        if trail == True:
            times2 = 1000 
            times3 = 1000
            tims4  = 1000 
        else:
            times2 = 19517
            times3 = 17272
            times4 = 99860    
                    
            
        trip_1d = np.zeros((220,1))     # data shows there are 220 hubwaystops as of july 2017
        trip_distance_pop = np.zeros((220,3))  # 220 stations with lat,lon,popularity

        #iterating over all three cvs files and storing data on our "trip_1d" and "trip_distance_pop"
        # after iteration some of the station's values(lat,lon,popularity) still may be zeros as these three cvs file doesn't contain
        #data for all 220 stops

        for index in range(times2):
            i=(data2.iloc[index,8])
            j = data2.iloc[index,3]
            
            trip_1d[i] = trip_1d[i]+1
            trip_1d[j] = trip_1d[j]+1
            
            #start(i's) lat long + popularity of i
            trip_distance_pop[i][0] = data2.iloc[index,9]
            trip_distance_pop[i][1] = data2.iloc[index,10]
            trip_distance_pop[i][2] = trip_1d[i]
            
            #end(j's) lat long + popularity of end
            trip_distance_pop[j][0] = data2.iloc[index,4]
            trip_distance_pop[j][1] = data2.iloc[index,5]
            trip_distance_pop[i][2] = trip_1d[j]
            
            

            
        for index in range(times3):    
            i=(data3.iloc[index,8])
            j = data3.iloc[index,3]
            
            trip_1d[i] = trip_1d[i]+1
            trip_1d[j] = trip_1d[j]+1
            
            
             #start(i's) lat long + popularity of i
            trip_distance_pop[i][0] = data3.iloc[index,9]
            trip_distance_pop[i][1] = data3.iloc[index,10]
            trip_distance_pop[i][2] = trip_1d[i]
            
            #end(j's) lat long + popularity of end
            trip_distance_pop[j][0] = data3.iloc[index,4]
            trip_distance_pop[j][1] = data3.iloc[index,5]
            trip_distance_pop[i][2] = trip_1d[j]
                    
        for index in range(times4):    
            i=(data4.iloc[index,8])
            j = data4.iloc[index,3]
            
            trip_1d[i] = trip_1d[i]+1
            trip_1d[j] = trip_1d[j]+1
            
            
             #start(i's) lat long + popularity of i
            trip_distance_pop[i][0] = data4.iloc[index,9]
            trip_distance_pop[i][1] = data4.iloc[index,10]
            trip_distance_pop[i][2] = trip_1d[i]
            
            #end(j's) lat long + popularity of end
            trip_distance_pop[j][0] = data4.iloc[index,4]
            trip_distance_pop[j][1] = data4.iloc[index,5]
            trip_distance_pop[i][2] = trip_1d[j]

        
        # removing all the stops that have zeros in all three fiels(lat,lon,popularity)
        remove_zero_pop = np.array([[]])
        counter = 0 



        
        for i in range(len(trip_1d)):
            if trip_1d[i][0] != float(0):
                remove_zero_pop = np.append(remove_zero_pop,[[trip_1d[i]]])
                counter = counter+1
                
                
        
        # removing all zeros
        remove_zero_trip_distance_pop = np.array([[]])
        counter = 0 

        for i in range(len(trip_distance_pop)):
            if trip_distance_pop[i][0] != 0:
                remove_zero_trip_distance_pop = np.append(remove_zero_trip_distance_pop,[[trip_distance_pop[i]]])
                counter = counter+1
                
                
        remove_zero_trip_distance_pop = remove_zero_trip_distance_pop.reshape((counter,3))

        if trail == True:
            times4 = counter 
        else:
            times4 = 181        

        print(times4)    
            
        remove_zero_pop = remove_zero_pop.reshape((times4,1))


        location_matrix = np.zeros((times4, 2))
        location_matrix[:,0]= remove_zero_trip_distance_pop[:,0]
        location_matrix[:,1] = remove_zero_trip_distance_pop[:,1]


        #####
        num_of_cluster = 20

        agg = AgglomerativeClustering(n_clusters=num_of_cluster, affinity=geo_affinity, linkage="average")
        label1 = agg.fit_predict(location_matrix)  # Returns class labels.
        #clustering based on distance

        key_value_data = np.concatenate((np.array([label1]).T, (remove_zero_trip_distance_pop)), axis=1)

        #aggregating clustrting with total popularity and average popularity of the cluster
        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [[key, f([p for (k,v1,v2,p) in R if k == key]),f([1 for (k,v1,v2,p) in R if k == key]),(f([p for (k,v1,v2,p) in R if k == key]))/(f([1 for (k,v1,v2,p) in R if k == key]))] for key in keys]

        #20 means
        keys = {r[0] for r in key_value_data}
        weighted_points = {math.floor(key):([[[v1,v2]]*math.floor(p) for (k,v1,v2,p) in key_value_data if k == key]) for key in keys}

         
        for i in range(num_of_cluster):    
            weighted_points[i]=[item for sublist in weighted_points[i] for item in sublist]

        keys = {r[0] for r in key_value_data}
        data2 = [([[v1,v2] for (k,v1,v2,p) in key_value_data if k == key]) for key in keys]


        counter=0
        old={}
        for x in data2:
            old[counter] = {
                    "type": "FeatureCollection",
                                                                                                    
                    "features": []}
            arr = []
            for i in x:
                print(i)
                elem = { "type": "Feature", "id": 1, "geometry": { "type": "Point", "coordinates": [i[1],i[0]] }}
                arr.append(elem) 
            print("//")
            old[counter]["features"]=arr
            counter = counter+1


        value = aggregate(key_value_data ,sum)
        json_pop = {str(i):[] for i in range(num_of_cluster) }


        def add_station(x,i):
            
            station=[]
            
            
            
            for j in range(i):
                index_of_max = (np.argmax(x, axis=0)[3])
                station.append(index_of_max)
                x[index_of_max][2] += 1
                x[index_of_max][3] = x[index_of_max][1]/x[index_of_max][2]
                
                inner_json = {str(l):[] for l in range(num_of_cluster)}
                for k in range(len(x)):
                    inner_json[str(k)]=x[k][3]
                    
                json_pop[str(j)]= inner_json   

            return station

        # in order where to put stations 
        iter1 = add_station(value,100)

        new_df = pd.DataFrame(json_pop)

        r = json.loads(new_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)

        jup_repo = client.repo

        repo.dropPermanent('solutionClusteringpopdb')
                        #repo.create_collection("trail_index")
        repo.createPermanent('solutionClusteringpopdb')
        repo['bm181354_rikenm.solutionClusteringpopdb'].insert_many(r)
        repo['bm181354_rikenm.solutionClusteringpopdb'].metadata({'complete':True})


        json_obj = {i:[] for i in range(num_of_cluster) }
        counter = [0]*num_of_cluster
        def kmean(X,n):
            kmeans = KMeans(n_clusters=n, random_state=0).fit(X)
            return kmeans.cluster_centers_

        final={}


        new =  {
                    "type": "FeatureCollection",
                                                                                                    
                    "features": []}


        counter2= 0    
        x=set() #set
        for i in iter1:
            arr = []
            counter[i] += 1
            X = np.array(weighted_points[i])
            
            
           
            json_obj[i] = kmean(X,counter[i])
            
            x= x|{i}
            
            print(x) 
            
            for j in x:
                 for k in range(len(json_obj[j])):
                     


                     elem = { "type": "Feature", "id": 1, "geometry": { "type": "Point", "coordinates": [json_obj[j][k][1],json_obj[j][k][0]] }}
                     arr.append(elem)
                    
            print("--")        
                  
            new["features"] = arr
            #print(counter2,arr)
            #print(new)
            
            final[counter2]={
               
                 "old":old,
                 "new":{
                    "type": "FeatureCollection",
                                                                                                    
                    "features": arr }
            
                
            }
           
            counter2 += 1




##        new_df = pd.DataFrame(final)
##
##        r = json.loads(new_df.to_json( orient='records'))
##        s = json.dumps(r, sort_keys=True, indent=2)
##
##        jup_repo = client.repo
##
##        repo.dropPermanent('solutionClusteringdb')
##                        #repo.create_collection("trail_index")
##        repo.createPermanent('solutionClusteringdb')
##        repo['bm181354_rikenm.solutionClusteringdb'].insert_many(r)
##        repo['bm181354_rikenm.solutionClusteringdb'].metadata({'complete':True})



        # logout
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}



##        @staticmethod
##        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
##            '''
##                Create the provenance document describing everything happening
##                in this script. Each run of the script will generate a new
##                document describing that invocation event.
##                '''
##            
            # Set up the database connection.
##            client = dml.pymongo.MongoClient()
##            repo = client.repo
##            
##            repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
##            doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
##            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
##            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
##            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
##            doc.add_namespace('bdp','http://datamechanics.io/?prefix=bm181354_rikenm/')
##            
##            this_script = doc.agent('alg:bm181354_rikenm#solutionClustering', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
##            
##            resource = doc.entity('bdp:htaindex_data_places_25', {'prov:label':'dataset of all indices raw values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
##            
##            get_index = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
##            
##            doc.wasAssociatedWith(get_index, this_script)
##            
##            #change this
##            doc.usage(get_index, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
##            
##            # change this
##            index = doc.entity('dat:bm181354_rikenm#solutionClusteringdb', {prov.model.PROV_LABEL:'index  of transportation, housing', prov.model.PROV_TYPE:'ont:DataSet'})
##            
##            doc.wasAttributedTo(index, this_script)
##            doc.wasGeneratedBy(index, get_index, endTime)
##            doc.wasDerivedFrom(index, resource, index, index, index)
##            
##            repo.logout()
##            return doc 
               

        





        





        

    
    
