import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from numpy.random import uniform
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
	reads = ['liwang_pyhsieh.nearest_hos_dist', 'liwang_pyhsieh.crash_spatial', 'liwang_pyhsieh.crash_2015']
	writes = ['liwang_pyhsieh.Constraint_satisfaction']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.now()
	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

		latitude_set = []
		longitude_set = []

		hospital_dis = repo['liwang_pyhsieh.nearest_hos_dist'].find()
		crash_spatial = repo['liwang_pyhsieh.crash_spatial'].find()
		crashes = repo['liwang_pyhsieh.crash_2015'].find()

		for dataobj in crashes:
			str_date = dataobj["Crash Date"] + " " + dataobj["Crash Time"]
			dateobj_date = datetime.strptime(str_date, "%d-%b-%Y %I:%M %p")
			if isNighttime(dateobj_date):
				latitude, longitude = (dataobj["Y Coordinate"], dataobj["X Coordinate"])
				latitude_set.append(latitude)
				longitude_set.append(longitude)

		latitude_min = min(latitude_set)
		latitude_max = max(latitude_set)
		longitude_min = min(longitude_set)
		longitude_max = max(longitude_set)

		#intersect_crash_distance = join(hospital_dis,crash_spatial, 1, 0, 2, 2)

		intersect_crash_distance = []
		for dataobj_crash in crash_spatial:
			for dataobj_distance in hospital_dis:
				if dataobj_distance["_id_crash"] == dataobj_crash["_id"]:
					intersect_crash_distance.append((dataobj_crash["_id"], dataobj_crash["location"], dataobj_distance["dist"]))
		found = 0
		result = {}

		while found < 5:
			random_hospital_location = generate_random(latitude_min,latitude_max,longitude_min,longitude_max)
			random_hospital_location = epsg2LonLat(random_hospital_location[1], random_hospital_location[0])
			
			total_score = 0

			for dataobj in intersect_crash_distance:
				new_distance = getVDist(random_hospital_location[0],random_hospital_location[1], dataobj[1]['coordinates'][0],dataobj[1]['coordinates'][1])
				score =  dataobj[2] - new_distance 

				if score > 0 :
					total_score += score

			if total_score > 0:
				result[str(found + 1)] = random_hospital_location
				found +=1
				total_score = 0

		repo.dropCollection("Constraint_satisfaction")
		repo.createCollection("Constraint_satisfaction")
		repo['liwang_pyhsieh.Constraint_satisfaction'].insert(result)

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

		resource_nearest_hos_dist = doc.entity('dat:liwang_pyhsieh#nearest_hos_dist', {prov.model.PROV_LABEL: 'nearest_hos_dist', prov.model.PROV_TYPE: 'ont:DataSet'})
		get_nearest_hos_dist = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_nearest_hos_dist, this_script)
		doc.usage(get_nearest_hos_dist,resource_nearest_hos_dist,startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


		resource_crash_spatial = doc.entity('dat:liwang_pyhsieh#crash_spatial', {prov.model.PROV_LABEL: 'crash_spatial', prov.model.PROV_TYPE: 'ont:DataSet'})
		get_crash_spatial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_crash_spatial, this_script)
		doc.usage(get_crash_spatial,resource_crash_spatial,startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

		get_constraint_satisfaction = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		constraint_satisfaction = doc.entity('dat:liwang_pyhsieh#constraint_satisfaction',
									{prov.model.PROV_LABEL: 'Find the location of hospitals that satisfise the constraint',
									prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(constraint_satisfaction, this_script)
		doc.wasGeneratedBy(constraint_satisfaction, get_constraint_satisfaction, endTime)
		doc.wasDerivedFrom(constraint_satisfaction, resource_nearest_hos_dist, get_constraint_satisfaction, get_constraint_satisfaction, get_constraint_satisfaction)
		doc.wasDerivedFrom(constraint_satisfaction, resource_crash_spatial, get_constraint_satisfaction, get_constraint_satisfaction, get_constraint_satisfaction)

		repo.logout()

		return doc


if __name__ == "__main__":
	Constraint_satisfaction.execute()
	doc = Constraint_satisfaction.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))










