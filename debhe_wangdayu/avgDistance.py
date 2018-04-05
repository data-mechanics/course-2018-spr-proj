import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

class avgDistance(dml.Algorithm):
	contributor = 'debhe_wangdayu'
	reads = ['debhe_wangdayu.busHubwayDistance', 'debhe_wangdayu.subwayHubwayDistance', 'debhe_wangdayu.schoolHubwayDistance']
	writes = ['debhe_wangdayu.avgDistance']

	'''
	This file will read the data in busStop table and hubwayStation table.
	By using these two table, we are trying to calculate which hubway station 
	is closest to a certain bus stop and what is their distance(by coordinates)
	'''
	@staticmethod
	def execute(trial = False):
		''' Merging data sets
		'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')
		
		
		# Loads the busHubwayDistance collection
		busHubwayDistance = []
		busHubwayDistance = repo['debhe_wangdayu.busHubwayDistance'].find()

		# Loads the schoolHubwayDistance collection
		schoolHubwayDistance = []
		schoolHubwayDistance = repo['debhe_wangdayu.schoolHubwayDistance'].find()

		# Loads the subwayHubwayDistance collection
		subwayHubwayDistance = []
		subwayHubwayDistance = repo['debhe_wangdayu.subwayHubwayDistance'].find()

		avgDistance = []
		totalDistance = 0
		count = 0
		for row in busHubwayDistance:
			totalDistance += row['Distance']
			count += 1
		avgBusHubDis = totalDistance / count
		avgDistance.append({'objectName': 'busHubway', 'avgDistance': avgBusHubDis})


		totalDistance = 0
		count = 0
		for row in subwayHubwayDistance:
			totalDistance += row['Distance']
			count += 1
		avgSubHubDis = totalDistance / count
		avgDistance.append({'objectName': 'subHubway', 'avgDistance': avgSubHubDis})


		totalDistance = 0
		count = 0
		for row in schoolHubwayDistance:
			totalDistance += row['Distance']
			count += 1
		avgSchoolHubDis = totalDistance / count
		avgDistance.append({'objectName': 'schoolHubway', 'avgDistance': avgSchoolHubDis})
		'''
		totalDistance = 0
		count = 0
		for row in restaurantHubwayDistance:
			totalDistance += row['Distance']
			count += 1
		avgResHubDis = totalDistance / count
		avgDistance.append({'objectName': 'restaurantHubway', 'avgDistance': avgResHubDis})
		'''


		# We are going to do the product of busStop collection and hubwayStation collection
		# When we are doing that, we are going to filter out all the un-useful station
		# We are going to record the bus Stop, the hubway stop and the distance between them
		# There are too much busStops in Boston, so we are going to calculate the first 2000 of them

				

		# We are going to create a table and save the records that we just calculated.
		repo.dropCollection("avgDistance")
		repo.createCollection("avgDistance")

		repo['debhe_wangdayu.avgDistance'].insert_many(avgDistance)
		repo['debhe_wangdayu.avgDistance'].metadata({'complete': True})
		print("Saved avgDistance", repo['debhe_wangdayu.avgDistance'].metadata())

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

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
		repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('ont',
						  'http://datamechanics.io/data/wuhaoyu_yiran123/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		

		this_script = doc.agent('alg:debhe_wangdayu#avgDistance',
								{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		resource = doc.entity('ont:MBTA_Bus_Stops',
											 {'prov:label': 'avg distance between bus stops and hubway',
											  prov.model.PROV_TYPE: 'geojson'})

		get_avgDistance = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_avgDistance, this_script)
		doc.usage(get_avgDistance, resource, startTime, None,
				  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

		avgDistance = doc.entity('dat:debhe_wangdayu#avgDistance',
						  {prov.model.PROV_LABEL: 'avg distance between bus, subway, schools and hubway',
						   prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(avgDistance, this_script)
		doc.wasGeneratedBy(avgDistance, get_avgDistance, endTime)
		doc.wasDerivedFrom(avgDistance, resource, get_avgDistance, get_avgDistance, get_avgDistance)

		repo.logout()

		return doc
