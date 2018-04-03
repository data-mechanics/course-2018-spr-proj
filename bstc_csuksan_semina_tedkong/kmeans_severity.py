# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 18:08:31 2018

@author: Alexander
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 16:32:25 2018

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
import matplotlib.pyplot as pyplot
import numpy as np
import scipy.io
import scipy.misc
from bokeh.io import output_file, show
from bokeh.models import (
  GMapPlot, GMapOptions, ColumnDataSource, Circle, Range1d, PanTool, WheelZoomTool, BoxSelectTool
)

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
        google_api_key = "AIzaSyBOG07I1r2Tn6Xye_eFgpZTbstjsj0kNfI"
        
        
        file = pd.read_json("merged_datasets/RestaurantRatingsAndHealthViolations_Boston.json", lines=True)
#        severity = file['ave_violation_severity']
#        rating = file['rating']
#        longitude = file['longitude']
#        latitude = file['latitude']
        arr = file[['ave_violation_severity', 'rating','latitude','longitude']].copy()
        #arr = np.array([severity, rating])
        #print(arr)
        k = 10
        #arr = data[data.longitude > -75]
        kmeans = sk.KMeans(n_clusters = k, random_state = 0).fit(arr[['ave_violation_severity','rating']])
        #print(kmeans.cluster_centers_)
        
        centroids = kmeans.cluster_centers_
        labels = kmeans.labels_
        
        data = np.array(arr[['ave_violation_severity','rating']])
        #arr = np.array(arr[['latitude','longitude']])
        #print(arr)
        #print(arr)
        
        
        map_options = GMapOptions(lat=42.35, lng=-71.05, map_type="roadmap", zoom=11)
        
        plot = GMapPlot(x_range=Range1d(), y_range=Range1d(), map_options=map_options)
        plot.title.text = "Boston"
        
        plot.api_key = google_api_key
        
        source = ColumnDataSource(
            data=dict(
                lat=np.array(arr[["latitude"]]),
                lon=np.array(arr[["longitude"]]),
            )
        )
        
        circle = Circle(x="lon", y="lat", size=15, fill_color="blue", fill_alpha=0.8, line_color=None)
        plot.add_glyph(source, circle)
        
        plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool())
        output_file("gmap_plot.html")
        show(plot)
        
        
        
        for i in range(k):
            # select only data observations with cluster label == i
            pyplot.figure(1)
            
            ds = data[np.where(labels==i)]
            # plot the data observations
            
            #pyplot.plot(ds[:,0],ds[:,1],'o')
            # plot the centroids
            #lines = pyplot.plot(centroids[i,0],centroids[i,1],'kx')
            # make the centroid x's bigger
            #pyplot.setp(lines,ms=15.0)
            #pyplot.setp(lines,mew=2.0)
        pyplot.show()
        
        """
        for i in range(k):
            # select only data observations with cluster label == i
            pyplot.figure(2)
            
            ds = arr[np.where(labels==i)]
            # plot the data observations
            
            pyplot.plot(ds[:,0],ds[:,1],'o')
            # plot the centroids
#            lines = pyplot.plot(centroids[i,0],centroids[i,1],'kx')
            # make the centroid x's bigger
#            pyplot.setp(lines,ms=15.0)
#            pyplot.setp(lines,mew=2.0)
        pyplot.show()
        """
        
        #print(type(file))
        
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
    
getBostonYelpRestaurantData.execute()
doc = getBostonYelpRestaurantData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
