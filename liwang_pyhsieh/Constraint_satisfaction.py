import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from numpy.random import uniform
from numpy import mean
import random
import math
from geopy.distance import vincenty

def isNighttime(dtobj):
	return dtobj.hour < 6 or dtobj.hour > 17

def dist(p1, p2):
    return pow(p1[0] - p2[0], 2) + pow(p1[1] - p2[1], 2)

def str2Datetime(date, time):
	return datetime.strptime(" ".join([date, time]), "%d-%b-%Y %I:%M %p")

def epsg2LonLat(x, y): # lon-lat
	inproj = Proj(init='epsg:26986')  # EPSG for MA
	outproj = Proj(init="epsg:4326")  # EPSG for world map
	return transform(inproj, outproj, x, y)

def generate_random(latitude_min,latitude_max,longitude_min,longitude_max):
	return (random.uniform(latitude_max,latitude_min), random.uniform(longitude_max,longitude_min))

def getVDist(lat1, long1, lat2, long2):
	return vincenty((lat1, long1), (lat2, long2)).kilometers

def join(S, R, s_index, r_index, s_val, r_val):
	result = []
	for s in S:
		for r in R:
			if s[s_index] == r[r_index]:
				result.append([ s[s_val], r[r_val] ])
	return result


class Constraint_satisfaction(dml.Algorithm):
	contributor = 'liwang_pyhsieh'
	reads = ['liwang_pyhsieh.crash_nearest_facilities', 'liwang_pyhsieh.crash_temporal_spatial']
	writes = ['liwang_pyhsieh.candidate_position_hospital']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.now()
	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

		nearest_facilities = [x for x in repo['liwang_pyhsieh.crash_nearest_facilities'].find()]
		crash_spatial = [x for x in repo['liwang_pyhsieh.crash_temporal_spatial'].find()]

		# Sort by id for join
		nearest_facilities = sorted(nearest_facilities, key=lambda x: x["_id"])
		crash_spatial = sorted(crash_spatial, key=lambda x: x["_id"])

		intersect_crash_facility_distance = []
		lat_range = []
		longt_range = []

		for idx, item in enumerate(crash_spatial):
			intersect_crash_facility_distance.append({
				"_id": item["_id"],
				"nearest_hospital_dist": nearest_facilities[idx]["nearest_hospital_dist"],
				"Long": item["location"]["coordinates"][0],
				"Lat": item["location"]["coordinates"][1]
			})
			lat_range.append(item["location"]["coordinates"][1])
			longt_range.append(item["location"]["coordinates"][0])

		longt_min, longt_max = min(longt_range), max(longt_range)
		lat_min, lat_max = min(lat_range), max(lat_range)

		# Each time, require some decrease by the same portion to previous result
		# Note the decrease_rate should not be to high, or it will loop forever
		found = 0
		results = []
		current_hospital_mindist = [x["nearest_hospital_dist"] for x in intersect_crash_facility_distance]
		total_decrease_rate = 0.8
		points_to_find = 5
		partial_decrease_rate = pow(total_decrease_rate, 1.0/points_to_find)
		average_dist_current = mean(current_hospital_mindist)

		while found < 5:
			random_hospital_location = generate_random(lat_min, lat_max, longt_min, longt_max)
			test_hospital_mindist = [x for x in current_hospital_mindist]

			for idx, dataobj in enumerate(intersect_crash_facility_distance):
				new_distance = getVDist(random_hospital_location[0], random_hospital_location[1], dataobj["Lat"], dataobj["Long"])
				if new_distance < current_hospital_mindist[idx]:
					test_hospital_mindist[idx] = new_distance
			
			average_dist_test = mean(test_hospital_mindist)
			decrease_rate = average_dist_test / average_dist_current

			if decrease_rate < partial_decrease_rate:
				results.append({"_id": found, "Lat": random_hospital_location[0], "Long": random_hospital_location[1]})
				found += 1
				current_hospital_mindist = test_hospital_mindist
				average_dist_current = average_dist_test

		repo.dropCollection("candidate_position_hospital")
		repo.createCollection("candidate_position_hospital")
		repo['liwang_pyhsieh.candidate_position_hospital'].insert(results)

		repo.logout()
		endTime = datetime.now()

		return {"start": startTime, "end": endTime}


	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

		this_script = doc.agent('alg:liwang_pyhsieh#Constraint_satisfaction',
								{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		resource_crash_nearest_facilities = doc.entity('dat:liwang_pyhsieh#crash_nearest_facilities', {prov.model.PROV_LABEL: 'Information about nearest hospital and police station', prov.model.PROV_TYPE: 'ont:DataSet'})
		get_crash_nearest_facilities = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_crash_nearest_facilities, this_script)
		doc.usage(get_crash_nearest_facilities, resource_crash_nearest_facilities, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


		resource_crash_temporal_spatial = doc.entity('dat:liwang_pyhsieh#crash_temporal_spatial', {prov.model.PROV_LABEL: 'Crash accident locations with spatial indexing and time', prov.model.PROV_TYPE: 'ont:DataSet'})
		get_crash_temporal_spatial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_crash_temporal_spatial, this_script)
		doc.usage(get_crash_temporal_spatial,resource_crash_temporal_spatial,startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

		get_candidate_position_hospital = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		candidate_position_hospital = doc.entity('dat:liwang_pyhsieh#constraint_satisfaction',
									{prov.model.PROV_LABEL: 'Candidate locations of hospitals that satisfies given constraint',
									prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(candidate_position_hospital, this_script)
		doc.wasGeneratedBy(candidate_position_hospital, get_candidate_position_hospital, endTime)
		doc.wasDerivedFrom(candidate_position_hospital, resource_crash_nearest_facilities, get_candidate_position_hospital, get_candidate_position_hospital, get_candidate_position_hospital)
		doc.wasDerivedFrom(candidate_position_hospital, resource_crash_temporal_spatial, get_candidate_position_hospital, get_candidate_position_hospital, get_candidate_position_hospital)

		repo.logout()

		return doc


if __name__ == "__main__":
	Constraint_satisfaction.execute()
	doc = Constraint_satisfaction.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))










