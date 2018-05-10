import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import csv
#import matplotlib.pyplot as plt
from math import sin, cos, sqrt, atan2, radians
from scipy.spatial.distance import pdist
import math
from collections import Counter
#import matplotlib.pyplot as plt
#from mpl_toolkits.mplot3d import Axes3D
from pymongo import MongoClient
import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

# what we are trying to solve! Hubway station is crowded throughBoston. We are trying to find the least popular stop that 
#has multiple stops nearby. We will use a clustering algorithm called AgglomerativeClustering.


class solutionLeastPopularStations(dml.Algorithm):
    contributor = 'bm181354_rikenm'
    reads = ['bm181354_rikenm.hubwayJuly','bm181354_rikenm.hubwayOne','bm181354_rikenm.hubwayTwo','bm181354_rikenm.hubwayThree']
    writes = ['bm181354_rikenm.solutionLeastPopularStationsdb','bm181354_rikenm.solutionClusteringpopdb']
    
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

        if trial == True:
            times = 50
            data = data.iloc[:50,:]
        else:
            times = 193
        

        location_matrix = np.zeros((times, 2))
        location_matrix[:,0]= data.iloc[0:]["Latitude"]
        location_matrix[:,1] = data.iloc[0:]["Longitude"]

        np.shape(geo_affinity(location_matrix))
        np.shape(location_matrix)

        # AgglomerativeClustering on these data based on their location. nearest neighbor are clustered together fiest
        
        agg = AgglomerativeClustering(n_clusters=20, affinity=geo_affinity, linkage="average")
        label1 = agg.fit_predict(location_matrix)  # Returns class labels.


        #clustering based on distance

        #plt.figure()
        #fig = plt.figure()
        #ax = fig.add_subplot(111, projection='3d')
        #ax.scatter(location_matrix[:,0], location_matrix[:,1], zs=0, zdir='z', s=20, c=label1, depthshade=True)
        #plt.show()

        # clustering based on popularity and distance
        
        #read data for hubway trips. We will used this to obtain popularity index of stops

        data2 = pd.DataFrame(list(trip_data1.find()))

        data3 = pd.DataFrame(list(trip_data2.find()))

        data4 = pd.DataFrame(list(trip_data3.find()))


        if trial == True:
            times2 = 1000   #only see 1000 data points
            times3 = 1000
            tims4  = 1000 
        else:
            times2 = 19517  #default has 19k data point
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


        #clustering based on popularity. similar popular stuff are combined first
            
        def relative_popularity_between_point(x,y):
            #difference between two 1d points. These point insignifies popularity of that node 
            #return (abs(x-y)/((x+1)/2))
            #sim = min(x,y)/max(x,y)#(x-y)**2
            difference = (x-y)**2
            return math.sqrt(difference)

        # removing all the stops that have zeros in all three fields(lat,lon,popularity)
        remove_zero_pop = np.array([[]])
        counter = 0 


        for i in range(len(trip_1d)):
            if trip_1d[i][0] != float(0):
                remove_zero_pop = np.append(remove_zero_pop,[[trip_1d[i]]])
                counter = counter+1
        remove_zero_pop = remove_zero_pop.reshape((181,1))


        #Pairwise distances between observations in n-dimensional space aka "pdist" but instead of distances it's popularity.
        def pop_affinity(M):
            return  np.array([[relative_popularity_between_point(a[0],b[0]) for a in M] for b in M])

        remove_zero_pop_minus = remove_zero_pop * -1    #as 
        remove_zero_pop_minus

        np.shape(pop_affinity(remove_zero_pop_minus))

        agg = AgglomerativeClustering(n_clusters=2, affinity=pop_affinity, linkage="average")
        agg.fit_predict(remove_zero_pop_minus)  # Returns class labels.    


        #final form. combining both distance and popularity index 
        #combining both geo and popularity. now each item represent pdist+ppopularity 
        def geo_pop_affinity(M):
            return  np.array([[relative_popularity_and_distance_between_point(a,b) for a in M] for b in M])

        def relative_popularity_and_distance_between_point(a,b):
            #score
            a_lat = a[0]
            a_lon = a[1]
            a_pop = a[2]

            a = [a_lat,a_lon]

            b_lat = b[0]
            b_lon = b[1]
            b_pop = b[2]

            b = [a_lat,a_lon]

            score = lat_dist(a,b) + relative_popularity_between_point(a_pop,b_pop)  #-

            return score

        # removing all zeros
        remove_zero_trip_distance_pop = np.array([[]])
        counter = 0 

        for i in range(len(trip_distance_pop)):
            if trip_distance_pop[i][0] != 0:
                remove_zero_trip_distance_pop = np.append(remove_zero_trip_distance_pop,[[trip_distance_pop[i]]])
                counter = counter+1


        remove_zero_trip_distance_pop = remove_zero_trip_distance_pop.reshape((counter,3))        


        remove_zero_trip_distance_pop=np.delete(remove_zero_trip_distance_pop, 132, 0)    
        #normalizing data
        xs=np.zeros((len(remove_zero_trip_distance_pop)))
        ys=np.zeros((len(remove_zero_trip_distance_pop)))
        zs=np.zeros((len(remove_zero_trip_distance_pop)))
        xs_mean = np.mean(remove_zero_trip_distance_pop[:,0])
        ys_mean = np.mean(remove_zero_trip_distance_pop[:,1])
        zs_mean = np.mean(remove_zero_trip_distance_pop[:,2])
        print(xs_mean,ys_mean,zs_mean)

        #normalizing data with mean. may be l2 is better
        for i in range(len(remove_zero_trip_distance_pop)):
            xs[i] = (remove_zero_trip_distance_pop[i][0]/xs_mean)   
            ys[i] = (remove_zero_trip_distance_pop[i][1]/ys_mean)
            zs[i] = (remove_zero_trip_distance_pop[i][2]/zs_mean)


        remove_zero_trip_distance_pop[:,0] /=   xs_mean
        remove_zero_trip_distance_pop[:,1] /=   ys_mean
        remove_zero_trip_distance_pop[:,2] /=   zs_mean



        '''plt.figure()
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(xs, ys, zs, zdir='z', s=20, c=None, depthshade=True)
        plt.show()'''

        agg = AgglomerativeClustering(n_clusters=15, affinity=geo_pop_affinity, linkage="average")
        labels = agg.fit_predict(remove_zero_trip_distance_pop) 


        '''plt.figure()
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(xs, ys, zs, zdir='z', s=20, c=labels, depthshade=True)
        plt.show()'''

        
        # combined all the computed data
        new_df = pd.DataFrame(
                         {'Latitude_normalized': remove_zero_trip_distance_pop[:,0],
                         'Longitude_normalized': remove_zero_trip_distance_pop[:,1],
                         'Popularity':remove_zero_trip_distance_pop[:,2] ,
                         'Y_label': labels

                         })

        r = json.loads(new_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)

        jup_repo = client.repo

        repo.dropPermanent('solutionLeastPopularStationsdb')
                #repo.create_collection("trail_index")
        repo.createPermanent('solutionLeastPopularStationsdb')
        repo['bm181354_rikenm.solutionLeastPopularStationsdb'].insert_many(r)
        repo['bm181354_rikenm.solutionLeastPopularStationsdb'].metadata({'complete':True})


        # finding least significant station

        count_of_labels=Counter(labels)
        label_with_most_stops=(count_of_labels).most_common(1)[0][0]


        #finding the station that is least significant

        threshold = 0.8
        d = dict(zip(range(len(xs)),[0]*len(xs)))

        def distance(a,b):
            return (math.sqrt((a[0]-b[0])**2+(a[1]-b[1])**2+(a[2]-b[2])**2))


        for i in range(len(xs)):
            for j in range(len(xs)):
                
                
                
                if labels[i] == label_with_most_stops:
                    a = [xs[i],ys[i],zs[i]]
                    b = [xs[j],ys[j],zs[j]]
                    dis = distance(a,b)
                    
                    
                    
                    
                    if dis < threshold:
                        d[i] += 1
                        
        smallest_index = max(d, key=d.get)
         
        
       # saving dictionary that says which points are least significant. Higher the value in the dictionary, least significant. Key is station and value = nearby neighbor with same value.
       

        
        
        # logout
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
        
        this_script = doc.agent('alg:bm181354_rikenm#solutionLeastPopularStations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:htaindex_data_places_25', {'prov:label':'dataset of all indices raw values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_index = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_index, this_script)
        
        #change this
        doc.usage(get_index, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        
        # change this
        index = doc.entity('dat:bm181354_rikenm#solutionLeastPopularStationsdb', {prov.model.PROV_LABEL:'index  of transportation, housing', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(index, this_script)
        doc.wasGeneratedBy(index, get_index, endTime)
        doc.wasDerivedFrom(index, resource, index, index, index)
        
        repo.logout()
        return doc

## eof







