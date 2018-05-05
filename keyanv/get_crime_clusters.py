import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import *
import ast


def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

class get_crime_clusters(dml.Algorithm):
    contributor = 'keyanv'
    reads = ['keyanv.crimes', 'keyanv.public_utilities']
    writes = ['keyanv.crime_clusters']


    @staticmethod
    def execute(trial=False, trial_num = 999):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        # Get crime incident locations
        crimes = repo['keyanv.crimes'].find()
        public_utilities = repo['keyanv.public_utilities'].find()

        coords_input = []

        for row in crimes:
            coord = ast.literal_eval(row['Location'])
            if coord[0] > 1:
                coords_input.append(coord)

        if trial: coords_input = coords_input[:trial_num]

        pub_util_coords = []
        for row in public_utilities:
            coord = (row['longitude'], row['latitude'])
            if type(coord[0]) == list:
                # for utilities with two coordinates, average the two together
                coord = ((coord[0][0]+coord[1][0])/2, (coord[0][1]+coord[1][1])/2)
            if coord[0] > 1:
                pub_util_coords.append(coord)
 
        # K-means algorithm, ends after at most 10 iterations
        prev_means = []
        i = 0
        # we can have at most 3 means
        next_means = [(0,0),(0,0),(0,0)]

        while prev_means != next_means and i<10:
            prev_means = next_means

            dist_to_mean = [(m, p, dist(m,p)) for (m, p) in product(next_means, coords_input)]
            dist_with_keys = [(p, dist(m,p)) for (m, p, d) in dist_to_mean]

            min_dist = aggregate(dist_with_keys, min)
            min_pairs = [(m, p) for ((m,p,d), (p2,d2)) in product(dist_to_mean, min_dist) if p==p2 and d==d2]

            tot_dist = aggregate(min_pairs, plus)

            mean_1 = [(m, 1) for (m, _) in min_pairs] 
            mean_c = aggregate(mean_1, sum)

            next_means = [scale(t,c) for ((m,t),(m2,c)) in product(tot_dist, mean_c) if m == m2]
            i += 1

        # filter the means down based on their distances
        filtered_means = []
        for mean in next_means:
            add = True
            for pub_util_coord in pub_util_coords:
                if dist(mean, pub_util_coord) < 0.4:
                    print(mean, pub_util_coord)
                    add = False
                    break
            if add:
                filtered_means.append(mean)

        crime_clusters = {"crime_clusters": filtered_means}
        print(crime_clusters)

        repo.dropCollection("crime_clusters")
        repo.createCollection("crime_clusters")

        repo['keyanv.crime_clusters'].insert_one(crime_clusters)
        repo['keyanv.crime_clusters'].metadata({'complete':True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv','keyanv')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/keyanv/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/keyanv/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


        this_script = doc.agent('alg:get_crime_clusters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('dat:crimes', {'prov:label': 'crimes', prov.model.PROV_TYPE:'ont:DataResource'})
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        crime_clusters = doc.entity('dat:crime_clusters', {prov.model.PROV_LABEL:'Crime Clusters',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_clusters, this_script)
        doc.wasGeneratedBy(crime_clusters, this_run, endTime)
        doc.wasDerivedFrom(crime_clusters, resource, this_run, this_run, this_run)

        repo.logout()

        return doc

get_crime_clusters.execute()
doc = get_crime_clusters.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




