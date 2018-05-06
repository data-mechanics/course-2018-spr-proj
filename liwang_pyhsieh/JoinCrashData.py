import json
import dml
import uuid
import prov.model
from datetime import datetime

# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class JoinCrashData(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.crash_temporal_spatial', 'liwang_pyhsieh.accident_density',
    'liwang_pyhsieh.crash_nearest_facilities', 'liwang_pyhsieh.safety_scores',
    'liwang_pyhsieh.crash_cluster_distribution']
    writes = ['liwang_pyhsieh.joined_crash_analysis']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        list_merged = [x for x in repo['liwang_pyhsieh.crash_temporal_spatial'].find().sort("_id", dml.pymongo.ASCENDING)]
        list_merged = sorted(list_merged, key=lambda x: x["_id"])

        for idx, item in enumerate(repo['liwang_pyhsieh.accident_density'].find().sort("_id", dml.pymongo.ASCENDING)):
            list_merged[idx]["accident_density"] = item["accident_density"]

        for idx, item in enumerate(repo['liwang_pyhsieh.crash_nearest_facilities'].find().sort("_id", dml.pymongo.ASCENDING)):
            list_merged[idx]["nearest_hospital"] = {
                "id": item["nearest_hospital_id"],
                "dist": item["nearest_hospital_dist"],
                "name": item["nearest_hospital_name"],
                "Long": item["nearest_hospital_coordinates"][0],
                "Lat": item["nearest_hospital_coordinates"][1]
            }
            list_merged[idx]["nearest_police"] = {
                "id": item["nearest_police_id"],
                "dist": item["nearest_police_dist"],
                "name": item["nearest_police_name"],
                "Long": item["nearest_police_coordinates"][0],
                "Lat": item["nearest_police_coordinates"][1]
            }
        for idx, item in enumerate(repo['liwang_pyhsieh.safety_scores'].find().sort("_id", dml.pymongo.ASCENDING)):
            list_merged[idx]["nearby_lights"] = item["lights"]
            list_merged[idx]["nearby_signals"] = item["signals"]
        
        for idx, item in enumerate(repo['liwang_pyhsieh.liwang_pyhsieh.crash_cluster_distribution'].find().sort("_id", dml.pymongo.ASCENDING)):
            list_merged[idx]["cluster_id"] = item["cluster_id"]
        
        repo.dropCollection("joined_crash_analysis")
        repo.createCollection("joined_crash_analysis")
        repo["liwang_pyhsieh.joined_crash_analysis"].insert_many(list_merged)

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
        this_script = doc.agent('alg:liwang_pyhsieh#JoinCrashData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_crash_temporal_spatial = doc.entity('dat:liwang_pyhsieh#crash_temporal_spatial', {prov.model.PROV_LABEL: 'Crash accident locations with spatial indexing and time', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_crash_temporal_spatial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_temporal_spatial, this_script)
        doc.usage(get_crash_temporal_spatial, resource_crash_temporal_spatial, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_accident_density = doc.entity('dat:liwang_pyhsieh#accident_density', {prov.model.PROV_LABEL: 'Accident density on each crash position', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_accident_density = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_accident_density, this_script)
        doc.usage(get_accident_density, resource_accident_density, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_crash_nearest_facilities = doc.entity('dat:liwang_pyhsieh#crash_nearest_facilities', {prov.model.PROV_LABEL: 'Information about nearest hospital and police station', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_crash_nearest_facilities = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_nearest_facilities, this_script)
        doc.usage(get_crash_nearest_facilities, resource_crash_nearest_facilities, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_safety_scores = doc.entity('dat:liwang_pyhsieh#safety_scores', {prov.model.PROV_LABEL: 'Counts for nearby lights and traffic signals for accidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_safety_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_safety_scores, this_script)
        doc.usage(get_safety_scores, resource_safety_scores, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_crash_cluster_distribution = doc.entity('dat:liwang_pyhsieh#crash_cluster_distribution', {prov.model.PROV_LABEL: 'Crash accident locations clustering result', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_crash_cluster_distribution = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_cluster_distribution, this_script)
        doc.usage(get_crash_cluster_distribution, resource_crash_cluster_distribution, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


        get_joined_crash_analysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        joined_crash_analysis = doc.entity('dat:liwang_pyhsieh#joined_crash_analysis',
            {prov.model.PROV_LABEL: 'Joined crash event related data for application querying', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(joined_crash_analysis, this_script)
        doc.wasGeneratedBy(joined_crash_analysis, get_joined_crash_analysis, endTime)
        doc.wasDerivedFrom(joined_crash_analysis, resource_crash_temporal_spatial, get_joined_crash_analysis, get_joined_crash_analysis, get_joined_crash_analysis)
        doc.wasDerivedFrom(joined_crash_analysis, resource_accident_density, get_joined_crash_analysis, get_joined_crash_analysis, get_joined_crash_analysis)
        doc.wasDerivedFrom(joined_crash_analysis, resource_crash_nearest_facilities, get_joined_crash_analysis, get_joined_crash_analysis, get_joined_crash_analysis)
        doc.wasDerivedFrom(joined_crash_analysis, resource_safety_scores, get_joined_crash_analysis, get_joined_crash_analysis, get_joined_crash_analysis)
        doc.wasDerivedFrom(joined_crash_analysis, resource_crash_cluster_distribution, get_joined_crash_analysis, get_joined_crash_analysis, get_joined_crash_analysis)

        repo.logout()

        return doc


if __name__ == "__main__":
    JoinCrashData.execute()
    doc = JoinCrashData.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))