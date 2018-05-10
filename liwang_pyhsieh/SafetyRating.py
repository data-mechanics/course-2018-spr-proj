import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from geopy.distance import vincenty
from rtree import index as rt_index

# Return distance between two points (Latitude, Longitude)
# The distance is represented in kilometers
def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers

def kmToRadius(kms):
    earthRadiusInkms = 6371
    return kms / earthRadiusInkms

# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class SafetyRating(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_temporal_spatial', 'liwang_pyhsieh.street_lights', 'liwang_pyhsieh.traffic_signals']
    writes = ['liwang_pyhsieh.safety_scores']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        # Get crash dataset
        # Structure: {_id, location{type, coordinates[2]}, time}
        dataset_crash = repo['liwang_pyhsieh.crash_temporal_spatial'].find()
        
        rt_index_facilities = rt_index.Index()
        # Join and caculate distance of pairs
        # Note: rtree insersion doesn't require unique id, and we can identify items by bind objects
        # so we don't handle id issue here
        for row in repo['liwang_pyhsieh.street_lights'].find():
            # The values are of string format
            tmp_obj = {"_id": int(row["OBJECTID"]), "ftype": "light", "Lat": float(row["Lat"]), "Long": float(row["Long"])}
            rt_index_facilities.insert(tmp_obj["_id"], (tmp_obj["Long"], tmp_obj["Lat"], tmp_obj["Long"], tmp_obj["Lat"]), tmp_obj)

        for row in repo['liwang_pyhsieh.traffic_signals'].find():
            tmp_obj = {
                "_id": row["properties"]["OBJECTID"],
                "ftype": "signal",
                "Long": row["geometry"]["coordinates"][0],
                "Lat": row["geometry"]["coordinates"][1]
            }
            rt_index_facilities.insert(tmp_obj["_id"], (tmp_obj["Long"], tmp_obj["Lat"], tmp_obj["Long"], tmp_obj["Lat"]), tmp_obj)
        
        search_radius = 1.0
        search_radius_rad = kmToRadius(search_radius)
        safetyscore = []

        for posdata in dataset_crash:
            search_result = list(rt_index_facilities.intersection(
                (posdata["location"]["coordinates"][0] - search_radius_rad, posdata["location"]["coordinates"][1] - search_radius_rad,
                 posdata["location"]["coordinates"][0] + search_radius_rad, posdata["location"]["coordinates"][1] + search_radius_rad
                ), objects=True
            ))
            # Split search reasult
            l_count, s_count = 0, 0
            for resdata in search_result:
                resdata_obj = resdata.object
                if getVDist(posdata["location"]["coordinates"][1], posdata["location"]["coordinates"][0],
                        resdata_obj["Lat"], resdata_obj["Long"]) <= search_radius:
                    if resdata_obj["ftype"] == "light":
                        l_count += 1
                    else:
                        s_count += 1
            safetyscore.append({"_id": posdata["_id"], "lights": l_count, "signals": s_count})
            
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

        crash_temporal_spatial = doc.entity('dat:liwang_pyhsieh#crash_temporal_spatial', {prov.model.PROV_LABEL: '2015 Massachusetts Crash Report, with global coordinate and formatted time', prov.model.PROV_TYPE: 'ont:DataSet'})
        street_lights = doc.entity('dat:liwang_pyhsieh#street_lights', {prov.model.PROV_LABEL: 'Boston street light locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        traffic_signals = doc.entity('dat:liwang_pyhsieh#traffic_signals', {prov.model.PROV_LABEL: 'Boston traffic signals locations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_temporal_spatial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_street_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_traffic_signals = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crash_temporal_spatial, this_script)
        doc.wasAssociatedWith(get_street_lights, this_script)
        doc.wasAssociatedWith(get_traffic_signals, this_script)

        doc.usage(get_crash_temporal_spatial, crash_temporal_spatial, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_street_lights, street_lights, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_traffic_signals, traffic_signals, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_safety_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        safety_scores = doc.entity('dat:liwang_pyhsieh#safety_scores', {prov.model.PROV_LABEL: 'Counts for nearby lights and traffic signals for accidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety_scores, this_script)
        doc.wasGeneratedBy(safety_scores, get_safety_scores, endTime)
        doc.wasDerivedFrom(safety_scores, crash_temporal_spatial, get_safety_scores, get_safety_scores, get_safety_scores)
        doc.wasDerivedFrom(safety_scores, street_lights, get_safety_scores, get_safety_scores, get_safety_scores)
        doc.wasDerivedFrom(safety_scores, traffic_signals, get_safety_scores, get_safety_scores, get_safety_scores)

        repo.logout()

        return doc

if __name__ == "__main__":
    SafetyRating.execute()
    doc = SafetyRating.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
