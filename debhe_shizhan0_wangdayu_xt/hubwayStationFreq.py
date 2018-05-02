import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

"""
This file will read four different collections
It will read the hubwayStation to get the information about hubwayStation
It will read schoolHubwayDistance, busHubwayDistance, subwayHubwayDistance
Then it will analysize how frequent does each hubway station shows up in the
each result collection.
"""

class hubwayStationFreq(dml.Algorithm):
	contributor = 'debhe_shizhan0_wangdayu_xt'
	reads = ['debhe_shizhan0_wangdayu_xt.hubwayStation', 'debhe_shizhan0_wangdayu_xt.schoolHubwayDistance', 'debhe_shizhan0_wangdayu_xt.busHubwayDistance', 'debhe_shizhan0_wangdayu_xt.subwayHubwayDistance']
	writes = ['debhe_shizhan0_wangdayu_xt.hubwayStationFreq']

	@staticmethod
	def execute(trial = False):
		''' Merging data sets
		'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		
		# connect to the database and then read them
		schoolHubwayDistance = []
		schoolHubwayDistance = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()

		hubwayStation = []
		hubwayStation = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()

		busHubwayDistance = []
		busHubwayDistance = repo['debhe_shizhan0_wangdayu_xt.busHubwayDistance'].find()

		subwayHubwayDistance = []
		subwayHubwayDistance = repo['debhe_shizhan0_wangdayu_xt.subwayHubwayDistance'].find()

		# Make the deepcopy of the data list for further use
		schoolHubwayDistance_1 = copy.deepcopy(schoolHubwayDistance)
		busHubwayDistance_1 = copy.deepcopy(busHubwayDistance)
		subwayHubwayDistance_1 = copy.deepcopy(subwayHubwayDistance)
		hubwayStation_1 = copy.deepcopy(hubwayStation)
		hubwayStation_2 = copy.deepcopy(hubwayStation)

		# It will read the hubway collection data
		# only select the station name as the keys
		# put them into the distionary
		hubway_key = {}
		for row in hubwayStation :
			hubway_key[ row['station'] ] = 0

		# iterate through the schooHubwayDistance
		# aggregate the number of times it shows up based on the key
		for row in schoolHubwayDistance_1:
			hubway_key[ row['hubwayStation'] ] = hubway_key[ row['hubwayStation'] ] + 1
		# iterate through the schooHubwayDistance
		# aggregate the number of times it shows up based on the key
		for row in busHubwayDistance_1:
			hubway_key[ row['hubwayStation'] ] = hubway_key[ row['hubwayStation'] ] + 1
		# iterate through the schooHubwayDistance
		# aggregate the number of times it shows up based on the key
		for row in subwayHubwayDistance_1:
			hubway_key[ row['hubwayStation'] ] = hubway_key[ row['hubwayStation'] ] + 1

		# Project the result
		# Goind to show the number of dock for each stop
		# The name of the time that a certain number of hubway station shows up
		result = []
		for key,value in hubway_key.items() :
			dic = {}
			dic['Station'] = key
			dic['number'] = value
			dic['numDock'] = 0
			result.append(dic)

		for row_1 in result:
			hubway_station_3 = copy.deepcopy(hubwayStation_2)
			for row_2 in hubway_station_3 :
				if(row_1['Station'] == row_2['station']):
					row_1['numDock'] = row_2['dock_num']

		# We are going to create a table and save the records that we just calculated.
		repo.dropCollection("hubwayStationFreq")
		repo.createCollection("hubwayStationFreq")

		repo['debhe_shizhan0_wangdayu_xt.hubwayStationFreq'].insert_many(result)
		repo['debhe_shizhan0_wangdayu_xt.hubwayStationFreq'].metadata({'complete': True})
		print("Saved hubwayStationFreq", repo['debhe_shizhan0_wangdayu_xt.hubwayStationFreq'].metadata())

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
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
		doc.add_namespace('ont',
						  'http://datamechanics.io/data/wuhaoyu_yiran123/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		

		this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#hubwayStationFreq',
								{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		resource = doc.entity('ont:MBTA_Bus_Stops',
											 {'prov:label': 'How frequent does Hubway station show up in the result collections',
											  prov.model.PROV_TYPE: 'geojson'})

		get_hubwayStationFreq = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_hubwayStationFreq, this_script)
		doc.usage(get_hubwayStationFreq, resource, startTime, None,
				  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

		hubwayStationFreq = doc.entity('dat:debhe_shizhan0_wangdayu_xt#hubwayStationFreq',
						  {prov.model.PROV_LABEL: 'How frequent does Hubway station show up in the result collections',
						   prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(hubwayStationFreq, this_script)
		doc.wasGeneratedBy(hubwayStationFreq, get_hubwayStationFreq, endTime)
		doc.wasDerivedFrom(hubwayStationFreq, resource, get_hubwayStationFreq, get_hubwayStationFreq, get_hubwayStationFreq)

		repo.logout()

		return doc
