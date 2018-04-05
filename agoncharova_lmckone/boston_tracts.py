import dml
import prov.model
import pandas as pd
import requests
import datetime
import json
import uuid
import io
import urllib.request as ur

class boston_tracts(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.boston_tracts']


	@staticmethod
	def execute(trial=False):
		startTime = datetime.datetime.now()

		url = "http://datamechanics.io/data/boston_tracts_3.json"
		with ur.urlopen(url) as url:
			data = json.loads(url.read().decode())
			print(type(data))

		# set up the database
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection('boston_tracts')
		repo.createCollection('boston_tracts')
		
		print("About to insert " + str(len(data)) + " data points into " + 'boston_tracts')
		repo['agoncharova_lmckone.boston_tracts'].insert_many(data)
		repo['agoncharova_lmckone.boston_tracts'].metadata( { 'complete': True } )
		
		repo.logout()

		endTime = datetime.datetime.now()

		return { "start" : startTime, "end" : endTime }

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		
		# TODO: should this be removed?
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:agoncharova_lmckone#boston_tracts', { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py' })
		resource = doc.entity('dat:boston_tracts', { 'prov:label':'Boston Census Tracts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json' })
		get_boston_tracts = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_tracts, this_script)

		doc.usage(get_boston_tracts, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' })

		boston_tracts = doc.entity('dat:agoncharova_lmckone#boston_tracts', {prov.model.PROV_LABEL:'Boston: Tracts', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_tracts, this_script)
		doc.wasGeneratedBy(boston_tracts, get_boston_tracts, endTime)
		doc.wasDerivedFrom(boston_tracts, resource, get_boston_tracts, get_boston_tracts, get_boston_tracts)
		repo.logout()

		return doc

# boston_tracts.execute()
