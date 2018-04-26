import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from numpy.random import uniform
from numpy import array
from sklearn.cluster import KMeans

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
class KMeansForAccidents(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_temporal_spatial']
    writes = ['liwang_pyhsieh.crash_cluster_medians', 'liwang_pyhsieh.crash_cluster_distribution']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        print("Crash K-means...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        crash_ids = []
        crash_positions = []
        # Pull out all, select, then project part of the columns
        for dataobj in repo['liwang_pyhsieh.crash_temporal_spatial'].find():
            crash_ids.append(dataobj["_id"])
            crash_positions.append(dataobj["location"]["coordinates"])

        crash_positions = array(crash_positions)

        # Run K-means
        kmeans = KMeans(5).fit(crash_positions)

        data_clustered = []
        for idx, cid in enumerate(crash_ids):
            tmp_data = {
                "_id": cid,
                "cluster_id": int(kmeans.labels_[idx]),
                "coordinates": {
                    "Long": float(crash_positions[idx][0]),
                    "Lat": float(crash_positions[idx][1]),
                }
            }
            data_clustered.append(tmp_data)

        data_medians = []
        for idx, m in enumerate(kmeans.cluster_centers_):
            data_medians.append({
                "_id": idx,
                "coordinates": {"Lat": m[0], "Long": m[1]}
            })
        '''
        data_clustered = []
        for m, p in MP:
            longt, lat = epsg2LonLat(p[1][1], p[1][0])
            tmp_data = {
                "_id": p[0],
                "cluster_id": m[0],
                "coordinates": {"Lat": lat, "Long": longt}
            }
            data_clustered.append(tmp_data)

        data_medians = []
        for m in medians:
            longt, lat = epsg2LonLat(m[1][1], m[1][0])
            data_medians.append({
                "_id": m[0],
                "coordinates": {"Lat": lat, "Long": longt}
            })
        '''

        # Store cluster subordination
        repo.dropCollection("crash_cluster_medians")
        repo.createCollection("crash_cluster_medians")
        repo['liwang_pyhsieh.crash_cluster_medians'].insert_many(data_medians)

        repo.dropCollection("crash_cluster_distribution")
        repo.createCollection("crash_cluster_distribution")
        repo['liwang_pyhsieh.crash_cluster_distribution'].insert_many(data_clustered)
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

        this_script = doc.agent('alg:liwang_pyhsieh#kMeansForAccidents', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crash_2015 = doc.entity('dat:liwang_pyhsieh#crash_2015', {prov.model.PROV_LABEL: '2015 Massachusetts Crash Report', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_2015, this_script)

        doc.usage(get_crash_2015, crash_2015, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_crash_clusters_median = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_clusters_median = doc.entity('dat:liwang_pyhsieh#crash_cluster_medians',
                                     {prov.model.PROV_LABEL: 'Clustered medians for crash events', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_clusters_median, this_script)
        doc.wasGeneratedBy(crash_clusters_median, get_crash_clusters_median, endTime)
        doc.wasDerivedFrom(crash_clusters_median, crash_2015, get_crash_clusters_median, get_crash_clusters_median, get_crash_clusters_median)

        get_crash_clusters_distribution = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_clusters_distribution = doc.entity('dat:liwang_pyhsieh#crash_clusters_distribution',
                                     {prov.model.PROV_LABEL: 'Crash accident locations clustering result', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_clusters_distribution, this_script)
        doc.wasGeneratedBy(crash_clusters_distribution, get_crash_clusters_distribution, endTime)
        doc.wasDerivedFrom(crash_clusters_distribution, crash_2015, get_crash_clusters_distribution, get_crash_clusters_distribution, get_crash_clusters_distribution)

        repo.logout()

        return doc

if __name__ == "__main__":
    KMeansForAccidents.execute()
    doc = KMeansForAccidents.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
