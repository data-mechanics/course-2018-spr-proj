# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 23:37:05 2018

@author: Alexander
"""



import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from time import sleep
import sample
import pandas as pd
import sklearn.cluster as sk
import sklearn.neural_network as sk2
import matplotlib.pyplot as pyplot
import matplotlib
import numpy as np
import scipy.stats
import scipy.io
import scipy.misc
from bokeh.io import output_file, show
from bokeh.models import (
  GMapPlot, GMapOptions, ColumnDataSource, Circle, Range1d, PanTool, WheelZoomTool, BoxSelectTool
)
import bokeh.palettes
import scipy.stats as sc
from sklearn import preprocessing

class getBostonYelpRestaurantData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getBostonYelpRestaurantData']
    
    
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
#        
#        client = dml.pymongo.MongoClient()
#        repo = client.repo
#        repo.authenticate('bstc_semina', 'bstc_semina')
        
        #collection = repo.bstc_semina.RestaurantRatingAndHealthViolations
        #cursor = collection.find({})
        google_api_key = ""
        
        
        
        """
        
        First read in the merged Rating and Health Violation data and form a 
        box around the city of Boston to cut out blatantly wrong data
        
        """
        
        
        file = pd.read_json("merged_datasets/RestaurantRatingsAndHealthViolations_Boston.json", lines=True)
        if trial == True:
            splitted = np.array_split(file, 3)
            file = splitted[0]
#        severity = file['ave_violation_severity']
#        rating = file['rating']
#        longitude = file['longitude']
#        latitude = file['latitude']
        arr = file[['ave_violation_severity', 'rating','latitude','longitude']].copy()
        #arr = np.array([severity, rating])
        #print(arr)
        top = [42.400549, -71.004839]
        bot = [42.226935, -71.129931]
        right = [42.326260, -70.921191]
        left = [42.282590, -71.194209]
        
        arr = arr[(arr.latitude <= top[0]) & (arr.latitude >= bot[0]) & (arr.longitude <= right[1]) & (arr.longitude >= left[1])]
       

        """
        
        Compute kmeans on the severity & rating
        Then compute kmeans on all four parameters
        The ini array can be used as initial positions for the kmeans, else 
        the method will just choose its own.
        
        """
        ini = np.array([[0.0,5.0],[3.0,5.0],[1.5,1.0]])
        k = 5
        
        datafr = arr.values
        #print(datafr)
        
        kmeans = sk.KMeans(n_clusters = k).fit(arr[['ave_violation_severity','rating']])
        kmeans2 = sk.KMeans(n_clusters = k).fit(arr[['ave_violation_severity','rating', 'latitude', 'longitude']])
        
        
        centroids = kmeans.cluster_centers_
        labels = kmeans.labels_
        
        centroids2 = kmeans2.cluster_centers_
        labels2 = kmeans2.labels_
        
        
        """
        
        Turn the Dataframes into numpy arrays
        
        """
        
        data = np.array(arr[['ave_violation_severity','rating']])
        arr = np.array(arr[['latitude','longitude']])


        """
        
        Computes spearman's rho on the average values for each rating
        Then compute spearman's rho on the average values for each whole number
        rating
        
        """
        
        spear = [np.average(data[np.where(data[:,1] == 1)][:,0]), np.average(data[np.where(data[:,1] == 1.5)][:,0]),
                      np.average(data[np.where(data[:,1] == 2)][:,0]), np.average(data[np.where(data[:,1] == 2.5)][:,0]),
                           np.average(data[np.where(data[:,1] == 3)][:,0]), np.average(data[np.where(data[:,1] == 3.5)][:,0]),
                                np.average(data[np.where(data[:,1] == 4)][:,0]), np.average(data[np.where(data[:,1] == 4.5)][:,0]),
                                     np.average(data[np.where(data[:,1] == 5)][:,0])]
        
        #print(spear)   
        spear2 = [np.average(data[np.where(data[:,1] == 1)][:,0]), 
                      np.average(data[np.where(data[:,1] == 2)][:,0]), 
                           np.average(data[np.where(data[:,1] == 3)][:,0]), 
                                np.average(data[np.where(data[:,1] == 4)][:,0]), 
                                     np.average(data[np.where(data[:,1] == 5)][:,0])]
                    
        print(scipy.stats.spearmanr(spear, range(9)))
        print(scipy.stats.spearmanr(spear2, range(5)))
        
        """
        
        This section is commented out because it requires a google api key.
        It shows a map of boston with the restaurants plotted and colored
        based on their kmeans centroid.
        
        """
        
        """
        map_options = GMapOptions(lat=42.35, lng=-71.05, map_type="roadmap", zoom=11)
        
        plot = GMapPlot(x_range=Range1d(), y_range=Range1d(), map_options=map_options)
        plot.title.text = "Boston"
        
        plot.api_key = google_api_key
        """
        
        
        pal = bokeh.palettes.PuOr5
        
        pal2 = []
        
        for i in range(k):
            d = data[np.where(labels == i)]
            av = np.average(d,axis = 0)
            val = - av[0] + av[1]
            pal2 += [val]
            #print(av)
            
        p = sc.rankdata(pal2,method='dense')

        
        #I'm not sure what this does. I forget, but I'm too scared to remove it.
        """
        for i in range(k):
            # select only data observations with cluster label == i
            #pyplot.figure(1)
            
            ds = arr[np.where(labels==i)]
            # plot the data observations
            source = ColumnDataSource(
            data=dict(
                lat=ds[:,0],
                lon=ds[:,1],
                )
            )   
           
            circle = Circle(x="lon", y="lat", size=15, fill_color=pal[p[i] - 1], fill_alpha=0.8, line_color="Black")
            plot.add_glyph(source, circle)
            #pyplot.plot(ds[:,0],ds[:,1],'o')
            # plot the centroids
            #lines = pyplot.plot(centroids[i,0],centroids[i,1],'kx')
            # make the centroid x's bigger
            #pyplot.setp(lines,ms=15.0)
            #pyplot.setp(lines,mew=2.0)
        #pyplot.show()
        """
        
        
        """
        
        This section is the part that added the dots and colors to the googles map
        
        """

        """
        for i in range(k):
            
            ds = arr[np.where(labels2==i)]
            source = ColumnDataSource(
            data=dict(
                lat=ds[:,0],
                lon=ds[:,1],
                )
            )   
           
            circle = Circle(x="lon", y="lat", size=15, fill_color=pal[p[i] - 1], fill_alpha=0.8, line_color="Black")
            plot.add_glyph(source, circle)
            
            
        
        plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool())
        output_file("gmap_plot.html")
        show(plot)
        """
        
        """
        
        1. The plot shows kmeans centroids and groups
        
        """
        
        for i in range(k):
            # select only data observations with cluster label == i
            pyplot.figure(1)
            
            ds = data[np.where(labels==i)]
            # plot the data observations
            
            pyplot.plot(ds[:,0],ds[:,1],'o', color=pal[p[i] - 1])
            # plot the centroids
            lines = pyplot.plot(centroids[i,0],centroids[i,1],'kx')
            # make the centroid x's bigger
            pyplot.setp(lines,ms=15.0)
            pyplot.setp(lines,mew=2.0)
        pyplot.plot(spear, np.arange(1,5.5,.5), '|', markersize=20, color="Green")
        pyplot.show()
        
        pyplot.figure(2)
        
        """
        
        These 2 for loops plot the restaurants on a latitude/longitude y/x
        The color red represent the rating for each retaurant below the rating
        Blue is for the restaurants above the ratings.
        
        """
        
        for i in range(8):
            
            set1 = 1.5 + (i * 0.5)
            set2 = 1 + (i * 0.5)
            
            dset1 = arr[np.where(data[:,1] >= set1)]
            dset2 = arr[np.where(data[:,1] <= set2)]
            ax = pyplot.subplot(2, 4, i+1)
            ax.plot(dset1[:,0],dset1[:,1],'o', color = "Red")
            ax.plot(dset2[:,0],dset2[:,1],'o', color = "Blue")
            ax.set_title(set2)
            
        
        pyplot.figure(3)
        
        for i in range(7):
            
            set1 = 2 + (i * 0.5)
            set3 = 1.5 + (i * 0.5)
            set2 = 1 + (i * 0.5)
            
            dset1 = arr[np.where(data[:,1] >= set1)]
            dset3 = arr[np.where(data[:,1] == set3)]
            dset2 = arr[np.where(data[:,1] <= set2)]

            ax = pyplot.subplot(2, 4, i+1)
            ax.plot(dset1[:,0],dset1[:,1],'o', color = "Red")
            ax.plot(dset3[:,0],dset3[:,1],'o', color = "Purple")
            ax.plot(dset2[:,0],dset2[:,1],'o', color = "Blue")
            ax.set_title(set2)
        
        
        """
        
        This plot is the same as the previous for loops but just for >4.5 and <1.5
        
        """
        
        pyplot.figure(4)
        dset1 = arr[np.where(data[:,1] >= 4.5)]
        #dset3 = arr[np.where(data[:,1] == 3)]
        dset2 = arr[np.where(data[:,1] <= 1.5)]
        pyplot.plot(dset1[:,0],dset1[:,1],'o', color = "Red")
        #pyplot.plot(dset3[:,0],dset3[:,1],'o', color = "Purple")
        pyplot.plot(dset2[:,0],dset2[:,1],'o', color = "Blue")
        
        
        




#        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return ({'start':startTime, 'end':endTime})
    
    
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://www.yelp.com/developers/')

        this_script = doc.agent('alg:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Yelp+Reviews&$select=_id,businesses,total,region'
                  }
                  )

        rate = doc.entity('dat:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_LABEL:'Yelp Ratings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()
                  
        return doc
    
getBostonYelpRestaurantData.execute(True)
doc = getBostonYelpRestaurantData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
