import json
import dml
import uuid
import prov.model
from datetime import datetime
import pandas
from pyproj import Proj, transform
from geopy.distance import vincenty


def project(s, mapping):
    result = []
    for obj_s in s:
        dicttmp = {}
        for idx in mapping:
            dicttmp[mapping[idx]] = obj_s[idx]
        result.append(dicttmp)
    return result


def selection(s, f):
    return [s for s in s if f(s)]


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


def group_aggcount(D, idx, g_col):
    res = pandas.DataFrame.from_records(D, idx).groupby([g_col]).size()
    return [{"_id": i, "count": r} for i, r in res.iteritems()]

def group_aggsum(D, idx, g_col, s_col):
    res = pandas.DataFrame.from_records(D, idx).groupby([g_col])[s_col].agg(["sum"])
    idxlist = res.index.values
    return [ {"_id": idxlist[i], "sum": row["sum"]} for i, row in enumerate(res.to_dict("records"))]

def rename(D, colname_old, colname_new):
    for item in D:
        if colname_old in item:
            item[colname_new] = item[colname_old]
            del item[colname_old]
    return


def str2Datetime(date, time):
    return datetime.strptime(" ".join([date, time]), "%d-%b-%Y %I:%M %p")


def isNighttime(dtobj):
    return dtobj.hour < 6 or dtobj.hour > 17


def epsg2LonLat(x, y):
    inproj = Proj(init='epsg:26986')  # EPSG for MA
    outproj = Proj(init="epsg:4326")  # EPSG for world map
    return transform(inproj, outproj, x, y)


# Return distance between two points (Latitude, Longitude)
# The distance is represented in kilometers
def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers


# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class SafetyRating(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_2015', 'liwang_pyhsieh.street_lights', 'liwang_pyhsieh.traffic_signals']
    writes = ['liwang_pyhsieh.safety_scores']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

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
                longitude, latitude = epsg2LonLat(dataobj["X Coordinate"], dataobj["Y Coordinate"])
                dataset_crash.append({
                    "_id": dataobj["Crash Number"],
                    "time": dateobj_date,
                    "lat": latitude,
                    "long": longitude
                })


        # Join and caculate distance of pairs
        dataset_lights = [
            {"_id": row["_id"], "Lat": row["latitude"], "Long": row["longitude"]}
            for row in repo['liwang_pyhsieh.street_lights'].find()
        ]

        prod_crash_lights = product(dataset_crash, dataset_lights, "crash", "light", "_id", "_id")

        prod_crash_lights = [
            {"_id": row["_id"], "_id_crash": row["crash__id"], "_id_light": row["light__id"],
             "dist": getVDist(row["crash_lat"], row["crash_long"], row["light_Lat"], row["light_Long"])}
            for row in prod_crash_lights
        ]
        # Select ones within some range
        near_lights = [ row for row in prod_crash_lights if row["dist"] < 0.5 ]

        # Aggregate by sum
        lightcounts = group_aggcount(near_lights, "_id", "_id_crash")
        lightcounts = lightcounts + [{"_id": row["_id"], "count": 0} for row in dataset_crash]
        lightcounts = [{"_id": int(row["_id"]), "light_count": int(row["sum"])} for row in
                        group_aggsum(lightcounts, "_id", "_id", "count")]

        # Do them again on signals
        dataset_signals = [
            {
                "_id": row["properties"]["OBJECTID"],
                "Long": row["geometry"]["coordinates"][0],
                "Lat": row["geometry"]["coordinates"][1]
            }
            for row in repo['liwang_pyhsieh.traffic_signals'].find()
        ]
        prod_crash_signals = product(dataset_crash, dataset_signals, "crash", "signal", "_id", "_id")
        prod_crash_signals = [
            {"_id": row["_id"], "_id_crash": row["crash__id"], "_id_light": row["signal__id"],
             "dist": getVDist(row["crash_lat"], row["crash_long"], row["signal_Lat"], row["signal_Long"])}
            for row in prod_crash_signals
        ]
        near_signals = [
            row for row in prod_crash_signals if row["dist"] < 0.5
        ]

        # Items with zero count will disappear, so zero-filling is required
        # Encoding problem may happen when handling integers not of python native types
        # (becuase pandas is used), so a conversion is made in advance
        signalcounts = group_aggcount(near_signals, "_id", "_id_crash")
        signalcounts = signalcounts + [{"_id": row["_id"], "count": 0} for row in dataset_crash]
        signalcounts = [{"_id": int(row["_id"]), "signal_count": int(row["sum"])} for row in group_aggsum(signalcounts, "_id", "_id", "count")]


        # Join them on _id
        safetyscore = join(lightcounts, signalcounts, "light", "signal", "_id", "_id", "_id", "_id")

        # Store the result
        repo.dropCollection("safety_scores")
        repo.createCollection("safety_scores")
        # repo['liwang_pyhsieh.crash_2015'].insert_many(data_lightsignalcount)
        repo['liwang_pyhsieh.safety_scores'].insert_many(safetyscore)
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

        this_script = doc.agent('alg:liwang_pyhsieh#safetyRating', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crash_2015 = doc.entity('dat:liwang_pyhsieh#crash_2015', {prov.model.PROV_LABEL: '2015 Massachusetts Crash Report', prov.model.PROV_TYPE: 'ont:DataSet'})
        street_lights = doc.entity('dat:liwang_pyhsieh#street_lights', {prov.model.PROV_LABEL: 'Boston street light locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        traffic_signals = doc.entity('dat:liwang_pyhsieh#traffic_signals', {prov.model.PROV_LABEL: 'Boston traffic signals locations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_street_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_traffic_signals = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crash_2015, this_script)
        doc.wasAssociatedWith(get_street_lights, this_script)
        doc.wasAssociatedWith(get_traffic_signals, this_script)

        doc.usage(get_crash_2015, crash_2015, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_street_lights, street_lights, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_traffic_signals, traffic_signals, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_safety_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        safety_scores = doc.entity('dat:liwang_pyhsieh#safety_scores', {prov.model.PROV_LABEL: 'Counts for nearby lights and traffic signals for accidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety_scores, this_script)
        doc.wasGeneratedBy(safety_scores, get_safety_scores, endTime)
        doc.wasDerivedFrom(safety_scores, crash_2015, get_safety_scores, get_safety_scores, get_safety_scores)
        doc.wasDerivedFrom(safety_scores, street_lights, get_safety_scores, get_safety_scores, get_safety_scores)
        doc.wasDerivedFrom(safety_scores, traffic_signals, get_safety_scores, get_safety_scores, get_safety_scores)

        repo.logout()

        return doc

if __name__ == "__main__":
    SafetyRating.execute()
    doc = SafetyRating.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
