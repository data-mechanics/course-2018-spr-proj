import json
import dml
import uuid
import prov.model
from datetime import datetime
import pandas
from pyproj import Proj, transform
from geopy.distance import vincenty
from numpy.random import uniform

def str2Datetime(date, time):
    return datetime.strptime(" ".join([date, time]), "%d-%b-%Y %I:%M %p")

def epsg2LonLat(x, y):
    inproj = Proj(init='epsg:26986')  # EPSG for MA
    outproj = Proj(init="epsg:4326")  # EPSG for world map
    return transform(inproj, outproj, x, y)

def isNighttime(dtobj):
    return dtobj.hour < 6 or dtobj.hour > 17

# Return distance between two points (Latitude, Longitude)
# The distance is represented in kilometers
def dist(p1, p2):
    return pow(p1[0] - p2[0], 2) + pow(p1[1] - p2[1], 2)

def plus(args):
    p = [0,0]
    for _, (x,y) in args:
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

# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class kmeansforaccidents(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_2015']
    writes = ['liwang_pyhsieh.crash_clustering']

    @staticmethod
    def execute(trial=False):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        dataset_crash = []

        # Pull out all, select, then project part of the columns
        for dataobj in repo['liwang_pyhsieh.crash_2015'].find():
            str_date = dataobj["Crash Date"] + " " + dataobj["Crash Time"]
            dateobj_date = datetime.strptime(str_date, "%d-%b-%Y %I:%M %p")
            if isNighttime(dateobj_date):
                latitude, longitude = (dataobj["X Coordinate"], dataobj["Y Coordinate"])
                dataset_crash.append((
                    dataobj["Crash Number"],
                    (latitude, longitude)
                ))

        # The example here uses the k-means algorithm in class
        # For generating random initial medians
        lat_col = [pos[0] for _, pos in dataset_crash]
        lat_min, lat_max = min(lat_col), max(lat_col)
        long_col = [pos[1] for _, pos in dataset_crash]
        long_min, long_max = min(long_col), max(long_col)
        num_cluster = 5
        medians = [(i, p) for i, p in enumerate(list(zip(uniform(lat_min, lat_max, num_cluster), uniform(long_min, long_max, num_cluster))))]
        points = dataset_crash
        medians_old = []
        MP = []
        iter_ct, iter_limit = 0, 20
        while medians_old != medians or iter_limit == iter_ct:
            medians_old = medians

            MPD = [(m, p, dist(m[1], p[1])) for (m, p) in product(medians, points)]
            PDs = [(p, d) for (m, p, d) in MPD]
            PD = aggregate(PDs, min)
            MP = [(m, p) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p[0] == p2[0] and d == d2]
            MT = aggregate(MP, plus)

            M1 = [(m, 1) for (m, _) in MP]
            MC = aggregate(M1, sum)

            medians = [(m[0], scale(t, c)) for ((m, t), (m2, c)) in product(MT, MC) if m == m2]
            medians.sort()
            iter_ct = iter_ct + 1

        # Now, MP records the result
        data_cluster = [
            {
                "_id": p[0],
                "cluster_id": m[0],
                "coordinate": {"Lat": epsg2LonLat(m[1][0], m[1][1])[0], "Long": epsg2LonLat(m[1][0], m[1][1])[1]}
            }
            for m, p in MP]
        for i in range(50):
            print(data_cluster[i])

        # Store the result
        repo.dropCollection("crash_clustering")
        repo.createCollection("crash_clustering")
        # repo['liwang_pyhsieh.crash_2015'].insert_many(data_lightsignalcount)
        repo['liwang_pyhsieh.crash_clustering'].insert_many(data_cluster)
        repo.logout()
    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        pass

KmeansForAccidentDist.execute()