import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
import csv
from pymongo import MongoClient   #change this 
import json
import scipy
import dml
from scipy.stats import linregress

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





# See readme for average Linkage Dendrograms analysis.













