import json
import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import math
import numpy as np

class clusterGrocery(dml.Algorithm):
    contributor = 'colinstu'
    reads = ['colinstu.combineneighborhoodpoverty', 'colinstu.grocerygoogleplaces']
    writes = ['colinstu.kmeans']

    def dist(p1, p2):
        # Euclidean distance formula
        return math.sqrt((p2[0]-p1[0])**2 + (p2[1]-p1[1])**2)

    def find_kmeans(grocery, k, trial):
        # Set up the lat and long coordinates
        point_list = []
        output_coll = []
        if trial:
            for row in grocery.find()[:3]:
                point_list.append([row['geometry']['location']['lng'], row['geometry']['location']['lat']])
        else:
            for row in grocery.find():
                point_list.append([row['geometry']['location']['lng'],row['geometry']['location']['lat']])

        kmeans = KMeans(n_clusters=k).fit(point_list)

        centroids = kmeans.cluster_centers_.tolist()

        for p1 in centroids:
            min_dist = 9999999
            for row in grocery.find():
                    p2 = (float(row['geometry']['location']['lng']), float(row['geometry']['location']['lat']))
                    distance = clusterGrocery.dist(p1,p2)
                    if distance < min_dist:
                        min_dist = distance
                        min_row = row

            output_row = {}
            #output_row['grocery'] = min_row
            output_row['closest centroid'] = p1
            output_coll.append(output_row.copy())

        return output_coll

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        repo.dropCollection("kmeans")
        repo.createCollection("kmeans")
        grocery = repo.colinstu.grocerygoogleplaces
        neighborhood = repo.colinstu.combineneighborhoodpoverty

        k = 3  # change to the number of centroids you want to generate

        r = clusterGrocery.find_kmeans(grocery, k, trial)

        repo['colinstu.kmeans'].insert_many(r)
        repo['colinstu.kmeans'].metadata({'complete': True})
        print(repo['colinstu.kmeans'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('gdp', 'https://maps.googleapis.com/maps/api/place/textsearch/')

        this_script = doc.agent('alg:colinstu#clustergrocery',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('gdp:wc8w-nujj',
                              {'prov:label': 'Statistical Analysis', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        kmeans = doc.entity('dat:colinstu#kmeans',
                                       {prov.model.PROV_LABEL: 'Perform K-means Clustering',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        getkmeans = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getkmeans, this_script)
        doc.usage(getkmeans, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        getgrocerygoogleplaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getgrocerygoogleplaces, this_script)
        doc.usage(getgrocerygoogleplaces, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        getcombineneighborhoodpoverty = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getcombineneighborhoodpoverty, this_script)
        doc.usage(getcombineneighborhoodpoverty, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        doc.wasAttributedTo(kmeans, this_script)
        doc.wasGeneratedBy(getkmeans, getgrocerygoogleplaces, endTime)
        doc.wasDerivedFrom(kmeans, resource, getgrocerygoogleplaces, getgrocerygoogleplaces, getgrocerygoogleplaces)
        doc.wasDerivedFrom(kmeans, resource, getcombineneighborhoodpoverty, getcombineneighborhoodpoverty, getcombineneighborhoodpoverty)
        repo.logout()
        return doc


clusterGrocery.execute()
doc = clusterGrocery.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
