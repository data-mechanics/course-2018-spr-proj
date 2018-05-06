import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import *
import random
from haversine import haversine
import ast
import copy

class utilities_vs_crime(dml.Algorithm):
    contributor = 'keyanv'
    reads = ['keyanv.crimes', 'keyanv.public_utilities']
    writes = ['keyanv.utilities_vs_crime']


    def avg(x): # Average
        return sum(x)/len(x)
    
    def correlation(x, y):
        assert len(x) == len(y)
        n = len(x)
        assert n > 0
        avg_x = utilities_vs_crime.avg(x)
        avg_y = utilities_vs_crime.avg(y)
        diffprod = 0
        xdiff2 = 0
        ydiff2 = 0
        for idx in range(n):
            xdiff = x[idx] - avg_x
            ydiff = y[idx] - avg_y
            diffprod += xdiff * ydiff
            xdiff2 += xdiff * xdiff
            ydiff2 += ydiff * ydiff
    
        return diffprod / sqrt(xdiff2 * ydiff2)

    @staticmethod
    def execute(trial=False, trial_num = 99):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        public_utilities = repo['keyanv.public_utilities'].find()
        crimes = repo['keyanv.crimes'].find()
        crime_coords = []
        # get locations of all crimes
        for row in crimes:
            coord = ast.literal_eval(row['Location'])
            if coord[0] > 1:
                crime_coords.append(coord)

        if trial: crime_coords = crime_coords[:trial_num]

        # get locations of all public utilities
        pub_util_coords = []
        for row in public_utilities:
            coord = (row['latitude'], row['longitude'])
            if type(coord[0]) == list:
                # for utilities with two coordinates, average the two together
                coord = ((coord[0][0]+coord[1][0])/2, (coord[0][1]+coord[1][1])/2)
            if coord[0] > 1:
                pub_util_coords.append((row['type'], coord))\

        # possible ranges
        skip = 0.1
        bins = 16
        ranges = []

        for i in range(bins):
            ranges.append([round((i+1)*skip, 2), 0])

        UTILS = ['mbta_stop', 'pool', 'open_space']
        # generate the intial dictionary
        distance_vs_frequency = {}
        for util in UTILS:
            # NOTE: ranges is a template, so a deep copy must be stored here
            distance_vs_frequency[util] = copy.deepcopy(ranges)

        # for each crime, calculate distance to closest public utility
        for crime in crime_coords:
            # calculate min distances
            min_dist = {'mbta_stop': float('inf'), 'pool': float('inf'), 'open_space': float('inf')}
            for pub_util in pub_util_coords:
                dist = haversine(pub_util[1], crime)

                if dist < min_dist[pub_util[0]]:
                    min_dist[pub_util[0]] = dist

            # bin the results into the corresponding ranges
            for util in UTILS:
                index = int(min_dist[util]//skip)
                if index >= bins:
                    index = -1
                distance_vs_frequency[util][index][1] += 1

        # generate statistics
        for util in UTILS:
            x_dist = []
            y_freq = []

            # reformat data
            for points in distance_vs_frequency[util]:
                if points[1] > 0:
                    x_dist.append(points[0])
                    y_freq.append(points[1])

            # get averages
            x_bar = sum(x_dist)/len(x_dist)
            y_bar = sum(y_freq)/len(y_freq)

            # get standard deviations
            S_x = 0
            for x in x_dist:
                S_x += (x-x_bar)**2
            S_x = sqrt(S_x/(len(x_dist)-1))
            
            S_y = 0
            for y in y_freq:
                S_y += (y-y_bar)**2
            S_y = sqrt(S_y/(len(y_freq)-1))

            # calculate correlation
            correlation = 0
            for i in range(len(x_dist)):
                correlation += (x_dist[i]-x_bar)*(y_freq[i]-y_bar)

            correlation /= S_x * S_y
            correlation /= len(x_dist) - 1
            print('The correlation between the distance to', util, 'and crime is:', str(correlation))


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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
 
        this_script = doc.agent('alg:keyanv#utilities_vs_crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Computation':'py'})
        resource_properties= doc.entity('dat:keyanv#utilities_vs_crime', {'prov:label':'MongoDB', prov.model.PROV_TYPE:'ont:DataResource'})
        get_stats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stats, this_script)
        doc.usage(get_stats, resource_properties, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        utilities_crime_corr = doc.entity('dat:keyanv#utilities_vs_crime', {prov.model.PROV_LABEL:'Calculated Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(utilities_crime_corr, this_script)
        doc.wasGeneratedBy(utilities_crime_corr, get_stats, endTime)
        doc.wasDerivedFrom(utilities_crime_corr, resource_properties, get_stats, get_stats, get_stats)

        repo.logout()    
        return doc

utilities_vs_crime.execute(False)
doc = utilities_vs_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
