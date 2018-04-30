#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 13:07:53 2018

@author: jlove
"""

import dml
import prov.model
import shapely.geometry
import datetime
import numpy as np
import uuid

def find_closest_centroids(samples, centroids):
    closest_centroids = []
    for sample in samples:
        distances = []
        i = 0
        for centroid in centroids:
            distance = np.sqrt(((sample[0]-centroid[0])**2) +((sample[1]-centroid[1])**2))
            distances += [(distance,i)]
            i += 1
            closest_centroids += [min(distances)[1]]

    return np.array(closest_centroids)

def get_centroids(samples, clusters):
    sample_nums = {}
    for cluster in clusters:
        sample_nums[cluster] = None

    sums = [np.array([0, 0]) for _ in range(len(sample_nums.keys()))]
    numbers = [0 for _ in range(len(sample_nums.keys()))]

    for i in range(len(samples)):
        numbers[clusters[i]] += 1
        sums[clusters[i]][0] += samples[i][0]
        sums[clusters[i]][1] += samples[i][1]
    print(numbers)
    for i in range(len(sums)):
        sums[i] = sums[i]/numbers[i]


    return np.array(sums)

class findEvacLocations(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.hydrants']
    writes = ['jlove.evacHubs']
    

    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        
        repo.dropCollection("evac_hubs")
        repo.createCollection("evac_hubs")
        
        hydrants = repo['jlove.hydrants'].find_one({})
        points = []
        for hydrant in hydrants['features']:
                point = shapely.geometry.shape(hydrant['geometry'])
                points += [[point.x, point.y]]
        
        shuffled = points.copy()
        np.random.shuffle(shuffled)
        centroids = np.array(shuffled[:5])
        iterations = 30 if not trial else 5
        for i in range(iterations):
            clusters = find_closest_centroids(points, centroids)
            centroids = get_centroids(points, clusters)
        
        json_centroids = []
        for centroid in centroids:
            json_centroids += [{'x': centroid[0], 'y': centroid[1]}]
        
        repo['jlove.evac_hubs'].insert_many(json_centroids)
        repo.logout()
        
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        #ACTUALLY IMPLEMENT THIS
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/dataset/')
        
        this_script = doc.agent('alg:jlove#countEvacLovations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('dat:jlove#hydrants', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        evac_loc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(evac_loc, this_script)
        doc.usage(this_script, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        evacLoc = doc.entity('dat:jlove#evacloc', {prov.model.PROV_LABEL:'Tentative Locations For Boston Flood Evacuation Centers', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(evacLoc, this_script)
        doc.wasGeneratedBy(evacLoc, evac_loc, endTime)
        doc.wasDerivedFrom(evacLoc, resource, evac_loc, evac_loc, evac_loc)


        repo.logout()
                  
        return doc