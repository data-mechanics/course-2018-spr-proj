import json
import dml
import uuid
import prov.model
from pyproj import Proj, transform
from geopy.distance import vincenty
from datetime import datetime
from rtree import index as rt_index
from functools import reduce
import numpy as np

def parseLoc(s):
    locpair = s.split("\n")[-1][1:-1].split(",")
    return float(locpair[0]), float(locpair[1])


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


# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class NearestFacilityDist(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_cluster_distribution',
             'liwang_pyhsieh.hospitals', 'liwang_pyhsieh.police_stations']

    writes = ['liwang_pyhsieh.accidentcluster_averagefacilitydist', 'liwang_pyhsieh.crash_nearest_facilities']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        dataset_crash = repo['liwang_pyhsieh.crash_cluster_distribution'].find()

        rtree_index_hospital = rt_index.Index()
        rtree_index_police = rt_index.Index()

        for dataobj in repo['liwang_pyhsieh.hospitals'].find():
            lat, longt = parseLoc(dataobj["Location"])
            tmp_obj = {
                "_id": dataobj["_id"],
                "name": dataobj["NAME"],
                "Lat": lat,
                "Long": longt
            }
            rtree_index_hospital.insert(tmp_obj["_id"], (longt, lat, longt, lat), tmp_obj)

        for dataobj in repo['liwang_pyhsieh.police_stations'].find():
            tmp_obj = {
                "_id": dataobj["OBJECTID"],
                "name": dataobj["NAME"],
                "Lat": dataobj["Y"],
                "Long": dataobj["X"]
            }
            rtree_index_police.insert(tmp_obj["_id"],
                (tmp_obj["Long"], tmp_obj["Lat"], tmp_obj["Long"], tmp_obj["Lat"]), tmp_obj)
        
        # Find the nearest hospital and police station
        crash_nearest_facilities = []
        for crashdata in dataset_crash:
            tmp_nearest_hospital = list(rtree_index_hospital.nearest(
                (crashdata["coordinates"]["Long"], crashdata["coordinates"]["Lat"],
                crashdata["coordinates"]["Long"], crashdata["coordinates"]["Lat"]),
                1, objects=True))[0].object
            tmp_nearest_police = list(rtree_index_police.nearest(
                (crashdata["coordinates"]["Long"], crashdata["coordinates"]["Lat"],
                crashdata["coordinates"]["Long"], crashdata["coordinates"]["Lat"]),
                1, objects=True))[0].object
            crash_nearest_facilities.append({
                "_id": crashdata["_id"], 
                "cluster_id": crashdata["cluster_id"],
                "nearest_hospital_id": tmp_nearest_hospital["_id"],
                "nearest_police_id": tmp_nearest_police["_id"],
                "nearest_hospital_dist": getVDist(
                    crashdata["coordinates"]["Lat"], crashdata["coordinates"]["Long"],
                    tmp_nearest_hospital["Lat"], tmp_nearest_hospital["Long"]),
                "nearest_police_dist": getVDist(
                    crashdata["coordinates"]["Lat"], crashdata["coordinates"]["Long"],
                    tmp_nearest_police["Lat"], tmp_nearest_police["Long"])
                })

        repo.dropCollection("crash_nearest_facilities")
        repo.createCollection("crash_nearest_facilities")
        repo['liwang_pyhsieh.crash_nearest_facilities'].insert_many(crash_nearest_facilities)

        # Group by cluster and compute average distance
        clustered_crashdata = {}
        clust_nearestfacilityave = []
        for crashdata in crash_nearest_facilities:
            if crashdata["cluster_id"] not in clustered_crashdata:
                clustered_crashdata[crashdata["cluster_id"]] = []
            clustered_crashdata[crashdata["cluster_id"]].append(crashdata)
        
        for cluster_key in clustered_crashdata:
            cluster_data = clustered_crashdata[cluster_key]
            avedist_hos = reduce(lambda r, i: r+i["nearest_hospital_dist"], cluster_data, 0) / (len(cluster_data) or -1)
            avedist_pol = reduce(lambda r, i: r+i["nearest_police_dist"], cluster_data, 0) / (len(cluster_data) or -1)
            clust_nearestfacilityave.append({
                "_id": cluster_key,
                "average_hospital_dist": avedist_hos,
                "average_policestation_dist": avedist_pol
            })

        repo.dropCollection("accidentcluster_averagefacilitydist")
        repo.createCollection("accidentcluster_averagefacilitydist")
        repo['liwang_pyhsieh.accidentcluster_averagefacilitydist'].insert_many(clust_nearestfacilityave)

        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:liwang_pyhsieh#nearestFacilityDist', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crash_cluster_distribution = doc.entity('dat:liwang_pyhsieh#crash_cluster_distribution', {prov.model.PROV_LABEL: 'Crash accident locations clustering result', prov.model.PROV_TYPE: 'ont:DataSet'})
        hospitals = doc.entity('dat:liwang_pyhsieh#hospitals', {prov.model.PROV_LABEL: 'Boston hospital information', prov.model.PROV_TYPE: 'ont:DataSet'})
        police_stations = doc.entity('dat:liwang_pyhsieh#police_stations', {prov.model.PROV_LABEL: 'Boston police station information', prov.model.PROV_TYPE: 'ont:DataSet'})


        get_crash_cluster_distribution = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_hospitals = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_police_stations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crash_cluster_distribution, this_script)
        doc.wasAssociatedWith(get_hospitals, this_script)
        doc.wasAssociatedWith(get_police_stations, this_script)

        doc.usage(get_crash_cluster_distribution, crash_cluster_distribution, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_hospitals, hospitals, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_police_stations, police_stations, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


        get_crash_nearest_facilities = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_nearest_facilities = doc.entity('dat:liwang_pyhsieh#crash_nearest_facilities', {prov.model.PROV_LABEL: 'Information about nearest hospital and police station', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_nearest_facilities, this_script)
        doc.wasGeneratedBy(crash_nearest_facilities, get_crash_nearest_facilities, endTime)
        doc.wasDerivedFrom(get_crash_nearest_facilities, crash_cluster_distribution, get_crash_nearest_facilities, get_crash_nearest_facilities, get_crash_nearest_facilities)
        doc.wasDerivedFrom(get_crash_nearest_facilities, hospitals, get_crash_nearest_facilities, get_crash_nearest_facilities, get_crash_nearest_facilities)
        doc.wasDerivedFrom(get_crash_nearest_facilities, police_stations, get_crash_nearest_facilities, get_crash_nearest_facilities, get_crash_nearest_facilities)

        get_facility_avedist = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        facility_avedist = doc.entity('dat:liwang_pyhsieh#accidentcluster_averagefacilitydist', {prov.model.PROV_LABEL: 'Counts for nearby lights and traffic signals for accidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(facility_avedist, this_script)
        doc.wasGeneratedBy(facility_avedist, get_facility_avedist, endTime)
        doc.wasDerivedFrom(facility_avedist, crash_cluster_distribution, get_facility_avedist, get_facility_avedist, get_facility_avedist)
        doc.wasDerivedFrom(facility_avedist, hospitals, get_facility_avedist, get_facility_avedist, get_facility_avedist)
        doc.wasDerivedFrom(facility_avedist, police_stations, get_facility_avedist, get_facility_avedist, get_facility_avedist)

        repo.logout()

        return doc

if __name__ == "__main__":
    NearestFacilityDist.execute()
    doc = NearestFacilityDist.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))