import json
import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import math


class kmeans(dml.Algorithm):
    contributor = 'fjansen'
    reads = ['fjansen.k311']
    writes = ['fjansen.kmeans']

    def dist(self, p1, p2):
        # Euclidean distance formula
        return math.sqrt((p2[0] - p1[0]) ** 2 + (p2[1] - p1[1]) ** 2)

    def find_kmeans(self, k311, k, trial):
        # Set up the lat and long coordinates
        point_list = []
        output_coll = []
        if trial:
            for row in k311.find()[:3]:
                point_list.append([float(row['Latitude']), float(row['Longitude'])])
        else:
            for row in k311.find():
                if float(row['Latitude']) and float(row['Longitude']):
                    point_list.append([float(row['Latitude']), float(row['Longitude'])])
                else:
                    print(row)

        kmeans2 = KMeans(n_clusters=k, max_iter=300, n_init=10).fit(point_list)

        centroids = kmeans2.cluster_centers_.tolist()

        return centroids

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')

        repo.dropCollection("kmeans")
        repo.createCollection("kmeans")
        k311 = repo.fjansen.k311

        k = 20  # change to the number of centroids you want to generate

        r = kmeans.find_kmeans(k311, k, trial)
        print(r)

        # repo['fjansen.kmeans'].insert_many({'means': r})
        # repo['fjansen.kmeans'].metadata({'complete': True})
        # print(repo['fjansen.kmeans'].metadata())

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
        repo.authenticate('fjansen', 'fjansen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('gdp', 'https://maps.googleapis.com/maps/api/place/textsearch/')

        this_script = doc.agent('alg:fjansen#cluster311',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('gdp:wc8w-nujj',
                              {'prov:label': 'Statistical Analysis', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        kmeans = doc.entity('dat:fjansen#kmeans',
                            {prov.model.PROV_LABEL: 'Perform K-means Clustering',
                             prov.model.PROV_TYPE: 'ont:DataSet'})
        getkmeans = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getkmeans, this_script)
        doc.usage(getkmeans, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get311places = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get311places, this_script)
        doc.usage(get311places, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        doc.wasAttributedTo(kmeans, this_script)
        doc.wasGeneratedBy(getkmeans, get311places, endTime)
        repo.logout()
        return doc


res = kmeans.execute()
doc = kmeans.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
