import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from geopy.distance import vincenty

def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers

def str2Datetime(date, time):
    return datetime.strptime(" ".join([date, time]), "%d-%b-%Y %I:%M %p")

def epsg2LonLat(x, y): # lon-lat
    inproj = Proj(init='epsg:26986')  # EPSG for MA
    outproj = Proj(init="epsg:4326")  # EPSG for world map
    return transform(inproj, outproj, x, y)

def isNighttime(dtobj):
    return dtobj.hour < 6 or dtobj.hour > 17

# Since mongodb uses the value stored in data as spatial search unit,
# We need to convert the value if we want to conduct range search based on unit other than radius
def kmToRadius(kms):
    earthRadiusInkms = 6371
    return kms / earthRadiusInkms

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
class TransformAccidentDensity(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_2015']
    writes = ['liwang_pyhsieh.crash_spatial', 'liwang_pyhsieh.accident_density']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        crash_2015_spatial = []

        # Pick all accidents occurs at night
        # We also transform the spatial data format to make it support mongodb spatial search
        # Since this dataset uses EPSG system, we need coordinate conversion as well
        if trial == True:
            for dataobj in repo['liwang_pyhsieh.crash_2015'].aggregate([{"$sample": {"size": 400}}]):
                str_date = dataobj["Crash Date"] + " " + dataobj["Crash Time"]
                dateobj_date = datetime.strptime(str_date, "%d-%b-%Y %I:%M %p")
                if isNighttime(dateobj_date):
                    longitude, latitude = epsg2LonLat(dataobj["X Coordinate"], dataobj["Y Coordinate"])
                    crash_2015_spatial.append({
                        "_id": dataobj["Crash Number"],
                        "location": {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        }
                    })
        else:
            for dataobj in repo['liwang_pyhsieh.crash_2015'].find():
                str_date = dataobj["Crash Date"] + " " + dataobj["Crash Time"]
                dateobj_date = datetime.strptime(str_date, "%d-%b-%Y %I:%M %p")
                if isNighttime(dateobj_date):
                    longitude, latitude = epsg2LonLat(dataobj["X Coordinate"], dataobj["Y Coordinate"])
                    crash_2015_spatial.append({
                        "_id": dataobj["Crash Number"],
                        "location": {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        }
                    })

        # Store the result
        repo.dropCollection("crash_spatial")
        repo.createCollection("crash_spatial")
        # repo['liwang_pyhsieh.crash_spatial'].create_index([("location.coordinates", dml.pymongo.GEOSPHERE)])
        repo['liwang_pyhsieh.crash_spatial'].insert_many(crash_2015_spatial)

        # Compute accident density
        accident_density = []
        density_dist = 3.0     

        # Compute accident density for given range
        for posdata in crash_2015_spatial:
            accident_density.append({
                "_id": posdata["_id"],
                "accident_density":
                    len(
                        [
                            x for x in crash_2015_spatial
                            if getVDist(posdata["location"]["coordinates"][1], posdata["location"]["coordinates"][0],
                            x["location"]["coordinates"][1], x["location"]["coordinates"][0]) <= density_dist
                            and posdata["_id"] != x["_id"]
                        ]
                    )
            })

            '''
            # We planned to use mongodb's spatial query utility, but doesn't get expected result
            # It's possible that the problem is caused by precision on convertion between radius and kilometer
            temp_density = repo['liwang_pyhsieh.crash_spatial'].find({
                "loc": {"$geoWithin": {"$centerSphere": [posdata["location"]["coordinates"], density_dist]}}
            }).count()
            accident_density.append({
                "_id": posdata["_id"],
                "accident_density": temp_density
            })
            '''

        repo.dropCollection("accident_density")
        repo.createCollection("accident_density")
        repo["liwang_pyhsieh.accident_density"].insert_many(accident_density)

        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # create document object and define namespaces
        this_script = doc.agent('alg:liwang_pyhsieh#transformAccidentDensity',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        # https://data.cityofboston.gov/resource/492y-i77g.json
        resource_crash_2015 = doc.entity('dat:liwang_pyhsieh#crash_2015', {prov.model.PROV_LABEL: 'crash_2015', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_2015, this_script)
        doc.usage(get_crash_2015, resource_crash_2015, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_crash_spatial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_spatial = doc.entity('dat:liwang_pyhsieh#crash_spatial',
            {prov.model.PROV_LABEL: 'Crash accident locations with spatial indexing', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(crash_spatial, this_script)
        doc.wasGeneratedBy(crash_spatial, get_crash_spatial, endTime)
        doc.wasDerivedFrom(crash_spatial, resource_crash_2015, get_crash_spatial, get_crash_spatial, get_crash_spatial)

        get_accident_density = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        accident_density = doc.entity('dat:liwang_pyhsieh#accident_density',
                                   {prov.model.PROV_LABEL: 'Accident density on each crash position',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(accident_density, this_script)
        doc.wasGeneratedBy(accident_density, get_accident_density, endTime)
        doc.wasDerivedFrom(accident_density, resource_crash_2015, get_accident_density, get_accident_density, get_accident_density)

        repo.logout()

        return doc


if __name__ == "__main__":
    TransformAccidentDensity.execute()
    doc = TransformAccidentDensity.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))