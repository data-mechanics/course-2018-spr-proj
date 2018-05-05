
import io
import numpy as np
import matplotlib.pyplot as plt
import dml
import ast
import copy
from math import *
from haversine import haversine
import time

UTILS = ['mbta_stop', 'pool', 'open_space']

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

# get locations of all public utilities
pub_util_coords = []
for row in public_utilities:
    coord = (row['latitude'], row['longitude'])
    if type(coord[0]) == list:
        # for utilities with two coordinates, average the two together
        coord = ((coord[0][0]+coord[1][0])/2, (coord[0][1]+coord[1][1])/2)
    if coord[0] > 1:
        pub_util_coords.append((row['type'], coord))
begin = time.time()
f = open('min_dists.csv', 'w+')
f.write('mbta_stop,pool,open_space\n')
# for each crime, calculate distance to closest public utility
for crime in crime_coords:
    # calculate min distances
    min_dist = {'mbta_stop': float('inf'), 'pool': float('inf'), 'open_space': float('inf')}
    print(crime)
    for pub_util in pub_util_coords:
        dist = haversine(pub_util[1], crime)

        if dist < min_dist[pub_util[0]]:
            min_dist[pub_util[0]] = dist
    f.write(str(min_dist['mbta_stop'])+','+str(min_dist['pool'])+','+str(min_dist['open_space'])+'\n')

f.close()
print(time.time()-begin)