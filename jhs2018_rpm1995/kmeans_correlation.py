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


class kmeans_correlation(dml.Algorithm):
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.kmeansdata']
    writes = ['jhs2018_rpm1995.observations']

    @staticmethod
    def execute(trial=False):
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

        open_countclust = get_fitpredict(df1[['open_count', 'latitude', 'longitude']], 9)
        crime_clust = get_fitpredict(df1[['crime_count', 'latitude', 'longitude']], 9)
        hubway_clust = get_fitpredict(df1[['hubway_count', 'latitude', 'longitude']], 9)
        charge_clust = get_fitpredict(df1[['charge_count', 'latitude', 'longitude']], 9)

        observation = {}
        score = adjusted_rand_score(open_countclust, crime_clust)
        # print(score)
        observation["Rand_Score"] = score
        db.dropCollection("observations")
        db.createCollection("observations")
        db['jhs2018_rpm1995.observations'].insert_one(observation).inserted_id
        db.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        # Create the provenance document describing everything happening
        # in this script. Each run of the script will generate a new
        # document describing that invocation event.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet',
        # 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/')  # Boston
        # Wicked Open Data
        doc.add_namespace('ab', 'https://data.boston.gov/dataset/boston-neighborhoods')  # Analyze Boston

        this_script = doc.agent('alg:jhs2018_rpm1995#Combine_Coordinates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # #######
        resource_kmeansdata = doc.entity('dat:jhs2018_rpm1995_kmeansdata',
                                          {
                                              prov.model.PROV_LABEL: 'Coordinates of Environment Friendly Assets in '
                                                                     'Grid Form',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_observation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                       {prov.model.PROV_LABEL: "Statitical analysis Data", prov.model.PROV_TYPE:
                                           'ont:Computation'})

        doc.wasAssociatedWith(get_observation, this_script)

        doc.usage(get_observation, resource_kmeansdata, startTime)

        # #######
        observations = doc.entity('dat:jhs2018_rpm1995_observations', {prov.model.PROV_LABEL: 'Statistical Analysis '
                                                                                              'Data',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(observations, this_script)
        doc.wasGeneratedBy(observations, get_observation, endTime)
        doc.wasDerivedFrom(observations, resource_kmeansdata, get_observation, get_observation, get_observation)

        repo.logout()

        return doc


# kmeans_correlation.execute()
# doc = kmeans_correlation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
