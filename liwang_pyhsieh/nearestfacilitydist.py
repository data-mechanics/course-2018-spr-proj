import json
import dml
import uuid
import prov.model
import datetime
import pandas
from pyproj import Proj, transform
from geopy.distance import vincenty

def parseLoc(str):
    locpair = str.split("\n")[-1][1:-1].split(",")
    return (float(locpair[0]), float(locpair[1]))

def Epsg2LonLat(x, y):
    inproj = Proj(init='epsg:26986')  # EPSG for MA
    outproj = Proj(init="epsg:4326")  # EPSG for world map
    return transform(inproj, outproj, x, y)

def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers

# Return distance between two points (Latitude, Longitude)
# The distance is represented in kilometers
def VDist(p1, p2):
    return vincenty(p1, p2).kilometers

def rename(D, colname_old, colname_new):
    for item in D:
        if colname_old in item:
            item[colname_new] = item[colname_old]
            del item[colname_old]
    return

def product(S, R, s_prefix, r_prefix, idx_s, idx_r):
    result = []
    for obj_s in S:
        for obj_r in R:
            dicttmp = {"_id": "_".join([s_prefix, str(obj_s[idx_s]), r_prefix, str(obj_r[idx_r])])}
            for key_s in obj_s:
                dicttmp[s_prefix + "_" + key_s] = obj_s[key_s]
            for key_r in obj_r:
                dicttmp[r_prefix + "_" + key_r] = obj_r[key_r]
            result.append(dicttmp)
    return result

def join(S, R, s_prefix, r_prefix, s_idx, mcol_s, mcol_r, mcol_new):
    result = []
    for obj_s in S:
        for obj_r in R:
            if obj_s[mcol_s] == obj_r[mcol_r]:
                dicttmp = {"_id": obj_s[s_idx]}
                for key_s in obj_s:
                    if key_s != mcol_s:
                        dicttmp[s_prefix + "_" + key_s] = obj_s[key_s]
                for key_r in obj_r:
                    if key_r != mcol_r:
                        dicttmp[r_prefix + "_" + key_r] = obj_r[key_r]
                dicttmp[mcol_new] = obj_s[mcol_s]
                result.append(dicttmp)
    return result

def group_aggsum(D, idx, g_col, s_col):
    res = pandas.DataFrame.from_records(D, idx).groupby([g_col])[s_col].agg(["sum"])
    idxlist = res.index.values
    return [ {"_id": idxlist[i], "sum": row["sum"]} for i, row in enumerate(res.to_dict("records"))]

def group_aggmin(D, idx, g_col, s_col):
    res = pandas.DataFrame.from_records(D, idx).groupby([g_col])[s_col].agg(["min"])
    idxlist = res.index.values
    return [ {"_id": idxlist[i], "min": row["min"]} for i, row in enumerate(res.to_dict("records"))]

def group_aggave(D, idx, g_col, s_col):
    res = pandas.DataFrame.from_records(D, idx).groupby([g_col])[s_col].agg(["mean"])
    idxlist = res.index.values
    return [ {"_id": idxlist[i], "ave": row["mean"]} for i, row in enumerate(res.to_dict("records"))]

# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class nearestfacilitydist(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_clustering',
             'liwang_pyhsieh.hospitals', 'liwang_pyhsieh.police_station']

    writes = ['liwang_pyhsieh.accidentcluster_averagefacilitydist']

    @staticmethod
    def execute(trial=False):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        dataset_crash = list(repo['liwang_pyhsieh.crash_clustering'].find())

        dataset_hospital, dataset_police = [], []
        for dataobj in repo['liwang_pyhsieh.hospitals'].find():
            lat, long = parseLoc(dataobj["Location"])
            dataset_hospital.append({
                "_id": dataobj["_id"],
                "name": dataobj["NAME"],
                "lat": lat,
                "long": long
            })

        for dataobj in repo['liwang_pyhsieh.police_station'].find():
            dataset_police.append({
                "_id": dataobj["OBJECTID"],
                "name": dataobj["NAME"],
                "lat": dataobj["Y"],
                "long": dataobj["X"]
            })

        prod_crash_hospital = product(dataset_crash, dataset_hospital, "crash", "hos", "_id", "_id")
        prod_crash_hospital = [
            {"_id": row["_id"], "_id_crash": row["crash__id"], "_id_hos": row["hos__id"],
             "dist": getVDist(row["crash_coordinate"]["Lat"], row["crash_coordinate"]["Long"], row["hos_lat"], row["hos_long"])}
            for row in prod_crash_hospital
        ]

        nearest_hos_dist = group_aggmin(prod_crash_hospital, "_id", "_id_crash", "dist")
        nearest_hos_dist = join(dataset_crash, nearest_hos_dist, "crash", "hosdist", "_id", "_id", "_id", "_id")
        avedist_hos_cluster = group_aggave(nearest_hos_dist, "_id", "crash_cluster_id", "hosdist_min")

        prod_crash_police = product(dataset_crash, dataset_police, "crash", "pol", "_id", "_id")
        prod_crash_police = [
            {"_id": row["_id"], "_id_crash": row["crash__id"], "_id_pol": row["pol__id"],
             "dist": getVDist(row["crash_coordinate"]["Lat"], row["crash_coordinate"]["Long"], row["pol_lat"], row["pol_long"])}
            for row in prod_crash_police
        ]

        nearest_pol_dist = group_aggmin(prod_crash_police, "_id", "_id_crash", "dist")
        nearest_pol_dist = join(dataset_crash, nearest_pol_dist, "crash", "poldist", "_id", "_id", "_id", "_id")
        avedist_pol_cluster = group_aggave(nearest_pol_dist, "_id", "crash_cluster_id", "poldist_min")

        nearestfacilityave = join(avedist_hos_cluster, avedist_pol_cluster, "hosdistave", "poldistave", "_id", "_id", "_id", "_id")

        nearestfacilityave = [
            { "_id": int(row["_id"]),
              "average_hospital_dist": float(row["hosdistave_ave"]),
              "average_policestation_dist": float(row["poldistave_ave"])
            }
            for row in nearestfacilityave]

        repo.dropCollection("accidentcluster_averagefacilitydist")
        repo.createCollection("accidentcluster_averagefacilitydist")
        repo['liwang_pyhsieh.accidentcluster_averagefacilitydist'].insert_many(nearestfacilityave)
        repo.logout()

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        pass

nearestfacilitydist().execute()