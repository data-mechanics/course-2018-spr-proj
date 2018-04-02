from sklearn.cluster import KMeans
from scipy.cluster.vq import kmeans, vq
import numpy as np
import json
from scipy import stats
import datetime
import itertools
import collections
import dml
import prov.model
import urllib.request
import uuid

class get_crimerate_clusters(dml.Algorithm):
        contributor = 'ashleyyu_bzwtong_xhug'
        reads = ['ashleyyu_bzwtong.crimerate']
        writes = ['ashleyyu_bzwtong_xhug.crimerate_clusters']

        @staticmethod
        def execute(trial=False, logging=True, cluster_divisor=300):

            startTime = datetime.datetime.now()

            if logging:
                print("in get_crimerate_clusters.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')


            crimerate = repo['ashleyyu_bzwtong.crimerate'].find()
            coords_input = [tuple(row['Location'].replace('(', '').replace(')', '').split(','))
                            for row in crimerate if row['Location'] != '(0.00000000, 0.00000000)'
                            and row['Location'] != '(-1.00000000, -1.00000000)' ]

            coords_input = [(float(lat), float(lon)) for (lat, lon) in coords_input]

            n_clusters = len(coords_input)//cluster_divisor
            X =  np.array(coords_input)
            # looks like [(lat, long), (lat, long), (lat, long)...]

            kmeans_output = KMeans(n_clusters, random_state=0).fit(X)
            centroids = kmeans_output.cluster_centers_.tolist()
            del(centroids[1])
            crimerate_clusters_dict = {'crimerate_clusters': centroids}

            repo.dropCollection("crimerate_clusters")
            repo.createCollection("crimerate_clusters")

            repo['ashleyyu_bzwtong.crimerate_clusters'].insert_one(crimerate_clusters_dict)
            repo['ashleyyu_bzwtong.crimerate_clusters'].metadata({'complete':True})

            repo.logout()

            endTime = datetime.datetime.now()

            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('ashleyyu_bzwtong','ashleyyu_bzwtong')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ashleyyu_bzwtong/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/ashleyyu_bzwtong/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

            #Agent
            this_script = doc.agent('alg:get_crimerate_clusters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resource
            resource = doc.entity('dat:crimerate', {'prov:label': 'Crimerate', prov.model.PROV_TYPE:'ont:DataResource'})

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)

            doc.used(this_run, resource, startTime)

            #New dataset
            crimerate_clusters = doc.entity('dat:crimerate_clusters', {prov.model.PROV_LABEL:'Crimerate Clusters',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimerate_clusters, this_script)
            doc.wasGeneratedBy(crimerate_clusters, this_run, endTime)
            doc.wasDerivedFrom(crimerate_clusters, resource, this_run, this_run, this_run)

            repo.logout()
            return doc

get_crimerate_clusters.execute()
doc = get_crimerate_clusters.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




