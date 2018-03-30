import json
from shapely.geometry import shape, Point


# depending on your version, use: from shapely.geometry import shape, Point

#read boston tracts from mongodb

#create points out of evictions

#create field that counts evictions within census tract

#create points out of crimes

#create field that counts crimes within census tract


import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

import pymongo
from bson.code import Code
import pprint


class count_evictions_crimes(dml.Algorithm):

	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.boston_tracts', 'agoncharova_lmckone.boston_crimes', 'agoncharova_lmckone.boston_evictions']
	writes = ['agoncharova_lmckone.boston_tract_counts']

	def aggregate(R,f):
		'''helper methods courtsey of Profesoor Andrei Lapets'''
		keys = {r[0] for r in R}
		return [(key, f([v for (k, v) in R if k == key])) for key in keys]

	def project(R, p):
		return [p(t) for t in R]

	def select(R, s):
		return [t for t in R if s(t)]

	def product(R, S):
		return [(t, u) for t in R for u in S]

	@staticmethod
	def execute(trial = False):
		'''Retrieve Boston Approved Building Permits dataset'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		repo.dropCollection("boston_tract_counts")
		repo.createCollection("boston_tract_counts")

		boston_tracts = repo['agoncharova_lmckone.boston_tracts']
		boston_crimes = repo['agoncharova_lmckone.boston_crimes']
		boston_evictions = repo['agoncharova_lmckone.boston_evictions']

		eviction_tracts = []

		print("geocoding evictions...")
		for feature in boston_tracts.find():
			polygon = shape(feature['geometry'])
			for eviction in boston_evictions.find():
				point = Point(eviction['longitude'], eviction['latitude'])
				if polygon.contains(point):
					geoid = feature['properties']['GEOID']
					eviction_tracts.append((geoid, 1))
		print("able to geocode evictions")

		#aggregate count of evictions by summing the ones
		evictions_by_tract = count_evictions_crimes.aggregate(eviction_tracts, sum)
		print("evictions counted by tract")


		crime_tracts = []

		print("geocoding crimes...")
		for feature in boston_tracts.find():
			polygon = shape(feature['geometry'])
			for crime in boston_crimes.find():
				point = Point(crime['Long'], crime['Lat'])
				if polygon.contains(point):
					geoid = feature['properties']['GEOID']
					crime_tracts.append((geoid, 1))
		print("crimes geocoded")

		crimes_by_tract = count_evictions_crimes.aggregate(crime_tracts, sum)
		print("crimes counted by tract")

		#merge the json field on evictions and crimes by geoid

		boston_tracts_with_counts = [x for x in boston_tracts.find()]

		#insert evictions field
		print("inserting evictions count...")
		for feature in boston_tracts_with_counts:
			matches = [eviction_tract[1] for eviction_tract in evictions_by_tract if eviction_tract[0] == feature['properties']['GEOID']]
			if matches:
				feature['properties']['evictions'] = float(matches[0])
			else:
				feature['properties']['evictions'] = 0
		print("evictions count inserted")

		print("inserting crimes count...")
		#insert crimes field
		for feature in boston_tracts_with_counts:
			matches = [crime_tract[1] for crime_tract in crimes_by_tract if crime_tract[0] == feature['properties']['GEOID']]
			if matches:
				feature['properties']['crimes'] = matches[0]
			else:
				feature['properties']['crimes'] = 0
		print("crimes count inserted")

		print("example:")
		print(boston_tracts_with_counts[0])

		print("inserting into the db...")
		#insert data into repo
		repo['agoncharova_lmckone.boston_tract_counts'].insert_many(boston_tracts_with_counts)
		repo['agoncharova_lmckone.boston_tract_counts'].metadata({'complete':True})
		#print(repo['agoncharova_lmckone.boston_permits'].metadata())
		print("data inserted into the db")

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		# '''
		# Create the provenance document describing everything happening
		# in this script. Each run of the script will generate a new
		# document describing that invocation event.
		# '''

		# # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:agoncharova_lmckone#count_evictions_crimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_count_evictions_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		evictions_input = doc.entity('dat:agoncharova_lmckone.boston_evictions',
			{prov.model.PROV_LABEL:'Evictions',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		crimes_input = doc.entity('dat:agoncharova_lmckone.boston_crimes',
			{prov.model.PROV_LABEL:'Crimes',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		output = doc.entity('dat:agoncharova_lmckone.boston_tract_counts',
			{prov.model.PROV_LABEL:'Count of evictions and crimes in each census tract', prov.model.PROV_TYPE:'ont:DataSet'})

		doc.wasAssociatedWith(get_count_evictions_crimes, this_script)

		doc.usage(get_count_evictions_crimes, evictions_input, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'}
		          )

		doc.usage(get_count_evictions_crimes, crimes_input, startTime, None,
          {prov.model.PROV_TYPE:'ont:Computation'}
          )

		doc.wasAttributedTo(output, this_script)
		doc.wasGeneratedBy(output, get_count_evictions_crimes, endTime)
		doc.wasDerivedFrom(output, evictions_input, get_count_evictions_crimes, get_count_evictions_crimes, get_count_evictions_crimes)
		doc.wasDerivedFrom(output, crimes_input, get_count_evictions_crimes, get_count_evictions_crimes, get_count_evictions_crimes)
		
		repo.logout()
				  
		return doc


count_evictions_crimes.execute()
#sf_evictions_constructions.provenance()