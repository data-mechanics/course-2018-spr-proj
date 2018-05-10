# ########################### ML Heavy Code here; K- Means, Correlations, Metrics, etc.#################################

import dml
from pymongo import MongoClient
import prov.model
import datetime
import json
import uuid
import folium
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from sklearn import datasets
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics import adjusted_rand_score


class kmeans_correlation():
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.kmeansdata']
    writes = ['jhs2018_rpm1995.observations']

    def __init__(self, k = 9):
        self.execute(k)
    def execute(self, k):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        # repo = client.repo

        print("Now running kmeans_correlation.py")

        # client = MongoClient()
        db = client.repo
        db.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')
        collection = db.jhs2018_rpm1995.kmeansdata
        df = pd.DataFrame(list(collection.find()))

        # dir_path = "C:/Users/Jonathan"
        dir_path = os.path.dirname(os.path.abspath(__file__))

        latitude = [0] * len(df)
        longitude = [0] * len(df)
        for i in range(len(df)):
            latitude[i] = df['coordinates'][i][0]
            longitude[i] = df['coordinates'][i][1]

        df['latitude'] = latitude
        df['longitude'] = longitude
        # df['reallat'] = reallat
        # df['reallat'] = reallat
        # df['reallong'] = reallong
        # df = df[(df.charge_count != 0)|(df.hubway_count != 0)|(df.open_count != 0)]

        df[['charge_count', 'hubway_count', 'open_count', 'latitude', 'longitude',
            'crime_count']] = MinMaxScaler().fit_transform(
            df[['charge_count', 'hubway_count', 'open_count', 'latitude', 'longitude', 'crime_count']])

        df1 = df[(df.crime_count != 0) & (df.open_count != 0)]
        # crime_count = df1[['crime_count']]
        df1 = df1.reset_index()

        df1['latitude'] = df1.latitude + 1.0
        df1['longitude'] = df1.longitude + 1.0
        df1['hubway_count'] = df1.hubway_count * 2 + 1.0
        df1['charge_count'] = df1.charge_count * 2 + 1.0
        df1['open_count'] = df1.open_count * 2 + 1.0
        df1['crime_count'] = df1.crime_count * 2 + 1.0
        df1 = df1.reset_index()

        def get_fitpredict(X, k):
            y_pred = KMeans(n_clusters=k, random_state=0)
            y_pred = y_pred.fit_predict(X)
            return y_pred

        open_countclust = get_fitpredict(df1[['open_count', 'latitude', 'longitude']], k)
        crime_clust = get_fitpredict(df1[['crime_count', 'latitude', 'longitude']], k)
        hubway_clust = get_fitpredict(df1[['hubway_count', 'latitude', 'longitude']], k)
        charge_clust = get_fitpredict(df1[['charge_count', 'latitude', 'longitude']], k)

        observation = {}
        score = adjusted_rand_score(open_countclust, crime_clust)
        # print(score)
        observation["Rand_Score"] = score

        def graphYpred(X, df, name):
            name = folium.Map(location=[42.3123, -71.1], zoom_start=11)
            colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
                      'beige', 'darkblue', 'darkgreen', 'cadetblue',
                      'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen',
                      'gray', 'black', 'lightgray']
            color = []
            for clust in X:
                color.append(colors[clust])
            df['colors'] = color
            for point in range(0, len(df)):
                folium.CircleMarker(df['coordinates'][point], fill=True, fill_color=df['colors'][point], color='grey',
                                    fill_opacity=0.7).add_to(name)
            return name

        graphYpred(open_countclust, df1, 'map').save('templates/openClust.html')
        graphYpred(crime_clust, df1, 'map1').save('templates/crimeClust.html')
        graphYpred(hubway_clust, df1, 'map2').save('templates/hubwayClust.html')
        graphYpred(charge_clust, df1, 'map3').save('templates/chargeClust.html')


        db.dropCollection("observations")
        db.createCollection("observations")
        db['jhs2018_rpm1995.observations'].insert_one(observation).inserted_id
        db.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}