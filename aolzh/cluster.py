import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats
import random
from sklearn.cluster import KMeans
from sklearn import preprocessing
import numpy as np
from vincenty import vincenty

class cluster(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkHouses','aolzh.NewYorkNormHouses']
    writes = ['aolzh.Cluster']

    @staticmethod
    def execute(trial = False):
        print("cluster")
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorknormhouses = repo.aolzh.NewYorkNormHouses
        newyorkhouses = repo.aolzh.NewYorkHouses
        
        norm_houses = newyorknormhouses.find()
        houses = newyorkhouses.find()

        test_data = []
        price = []
        name = []
        for n_h in norm_houses:
            for h in houses:
                if n_h['house'] == h['address']:
                    location = [float(h['latitude']),float(h['longitude'])]
                    price.append(h['Price'])
                    name.append(h['address'])
                    break
            # 30% price 20% crime 20% subway 10% school 10% stores 10% hospitals
            houses_score = (n_h['norm_rate']*0.3+ n_h['norm_crime']*0.2 + n_h['norm_subway']*0.2 + n_h['norm_school']*0.1 + n_h['norm_stores']*0.1 + n_h['norm_hospitals']*0.1)

            test_data.append([houses_score,location[0],location[1]])

        testdata = np.array(test_data)

        score = testdata[:,0]
        latitude = testdata[:,1]
        longitude = testdata[:,2]

        testdata = preprocessing.scale(testdata)
        testdata[:, 1] = testdata[:,1] * 2.5
        testdata[:, 2] = testdata[:,2] * 2.5


        #To determine the k value
        error = float("inf")
        cluster_num = 10
        """
        for k in range(1,20):
            kmeanstmp = KMeans(init='k-means++', n_clusters=k, n_init=10)
            kmeanstmp.fit_predict(testdata)
            if kmeanstmp.inertia_ < error:
                error = kmeanstmp.inertia_
                cluster_num = k
        print(cluster_num)
        """
        kmeans = KMeans(n_clusters = cluster_num, init = 'k-means++', max_iter = 100, n_init = 10,random_state = 0)
        kmeans.fit_predict(testdata)
        k_centers = kmeans.cluster_centers_
        k_labels = kmeans.labels_
        k_error = kmeans.inertia_

        k_cluster_index = []
        for i in range(cluster_num):
            k_cluster_index.append([])
        for i in range(len(k_labels)):
            k_cluster_index[k_labels[i]].append(i)

        label_name = []
        cluster_house_name = []
        cluster_latitude = []
        cluster_longitude = []

        for i in range(cluster_num):
            total_score = 0
            total_price = 0
            cluster_house_name.append([])
            cluster_latitude.append([])
            cluster_longitude.append([])
            for j in range(len(k_cluster_index[i])):
                total_score += score[k_cluster_index[i][j]]
                total_price += price[k_cluster_index[i][j]]
                cluster_longitude[i].append(longitude[k_cluster_index[i][j]])
                cluster_latitude[i].append(latitude[k_cluster_index[i][j]])
                cluster_house_name[i].append(name[k_cluster_index[i][j]])
            label_name.append([total_score/len(k_cluster_index[i]), total_price/len(k_cluster_index[i])])

        res = []
        for i in range(cluster_num):
            for j in range(len(k_cluster_index[i])):
                
                res.append({'label':label_name[i],'name': cluster_house_name[i][j],'latitude':cluster_latitude[i][j],'longitude':cluster_longitude[i][j]})

        repo.dropCollection("Cluster")
        repo.createCollection("Cluster")
        repo["aolzh.Cluster"].insert_many(res)
        print("Finished")

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
        repo.authenticate('aolzh', 'aolzh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        cluster_script = doc.agent('alg:aolzh#cluster', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_cluster = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorknormhouses_resource = doc.entity('dat:aolzh#NewYorkNormHouses', {prov.model.PROV_LABEL:'NewYork Norm Houses', prov.model.PROV_TYPE:'ont:DataSet'})
        newyorkhouses_resource = doc.entity('dat:aolzh#NewYorkHouses', {prov.model.PROV_LABEL:'NewYork Houses', prov.model.PROV_TYPE:'ont:DataSet'})

        cluster_ = doc.entity('dat:aolzh#Cluster', {prov.model.PROV_LABEL:'NewYork Houses Cluster', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(get_cluster, cluster_script)

        doc.usage(get_cluster, newyorknormhouses_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        doc.usage(get_cluster, newyorkhouses_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        doc.wasAttributedTo(cluster_, cluster_script)
        doc.wasGeneratedBy(cluster_, get_cluster, endTime)
        doc.wasDerivedFrom(cluster_, newyorknormhouses_resource,get_cluster, get_cluster, get_cluster)
        doc.wasDerivedFrom(cluster_, newyorkhouses_resource,get_cluster, get_cluster, get_cluster)

        repo.logout()
                  
        return doc

cluster.execute()
doc = cluster.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
