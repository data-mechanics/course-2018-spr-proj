import json
from shapely.geometry import shape, Point
import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

import pymongo
from bson.code import Code
import pprint

###eventually just edit get_all_counts

##inserts score and makes into a proper geojson

class insert_score(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.stability_score', 'agoncharova_lmckone.boston_tracts_all_counts']
	writes = ['agoncharova_lmckone.boston_tracts_scores']

	@staticmethod
	def execute(trial = False):
		'''Inserts income into the repo'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection("boston_tracts_scores")
		repo.createCollection("boston_tracts_scores")

		boston_tract_counts = repo['agoncharova_lmckone.boston_tracts_all_counts']
		scores_list = repo['agoncharova_lmckone.stability_score']
		scores = [{'GEOID': x['Tract'],'score': x['stability']} for x in scores_list.find()]




		#copy collection into a fresh list that we will manipulate and reinsert into the db
		boston_tracts_scores = [x for x in boston_tract_counts.find()]

		#insert incomes into the census tracts collection, in 'properties'
		print("inserting scores")
		for feature in boston_tracts_scores:
			matches = [score['score'] for score in scores if score['GEOID'] == feature['properties']['GEOID'] and score['score'] is not None]
			if matches:
				feature['properties']['score'] = float(matches[0])
			else:
				feature['properties']['score'] = 0

		print("scores inserted")
		#print(boston_tracts_scores.keys())
		print(len(boston_tracts_scores))

		for x in boston_tracts_scores:
			del x['_id']



		#write to the db

		repo['agoncharova_lmckone.boston_tracts_scores'].insert_many(boston_tracts_scores)
		repo['agoncharova_lmckone.boston_tracts_scores'].metadata({'complete':True})

		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		# Set up the database connection.
		
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		# TODO: should this be removed?
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:agoncharova_lmckone#insert_score', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_all_counts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		score_input = doc.entity('dat:agoncharova_lmckone.stability_score',
			{prov.model.PROV_LABEL:'Income',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		tracts_input = doc.entity('dat:agoncharova_lmckone.boston_tract_counts',
			{prov.model.PROV_LABEL:'Census Tract Information',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		output = doc.entity('dat:agoncharova_lmckone.boston_tracts_scores',
			{prov.model.PROV_LABEL:'Count of evictions, crimes, and income in each census tract', prov.model.PROV_TYPE:'ont:DataSet'})

		doc.wasAssociatedWith(insert_score, this_script)

		doc.usage(insert_score, score_input, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'}
		          )

		doc.usage(get_all_counts, tracts_input, startTime, None,
          {prov.model.PROV_TYPE:'ont:Computation'}
          )

		doc.wasAttributedTo(output, this_script)
		doc.wasGeneratedBy(output, insert_score, endTime)
		doc.wasDerivedFrom(output, income_input, insert_score, insert_score, insert_score)
		doc.wasDerivedFrom(output, tracts_input, insert_score, insert_score, insert_score)
		
		repo.logout()
				  
		return doc

insert_score.execute()
