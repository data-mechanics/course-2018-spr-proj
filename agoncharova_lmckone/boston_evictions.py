import dml
import prov.model
import pandas as pd
import requests
import datetime
import json
import uuid
import io

class boston_evictions(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.boston_evictions']
	coll_name = "boston_evictions"

	@staticmethod
	def get_data():
		url = "http://datamechanics.io/data/evictions_boston.csv"
		data = pd.read_csv(url)
		return data


	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		b_e = boston_evictions

		evictions_dict = b_e.get_data().to_dict('records')

		# set up the database
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection(b_e.coll_name)
		repo.createCollection(b_e.coll_name)
		
		if(trial):
			evictions_dict = evictions_dict[:1000]
		
		print("About to insert " + str(len(evictions_dict)) + " data points into " + b_e.coll_name)
		repo[b_e.writes[0]].insert_many(evictions_dict)
		repo[b_e.writes[0]].metadata( { 'complete': True } )
		print(repo[b_e.writes[0]].metadata())
		
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

		this_script = doc.agent('alg:agoncharova_lmckone#boston_evictions', { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py' })
		resource = doc.entity('bdp:ef17538e-7718-409f-baad-bfc0b68704e5', { 'prov:label':'Boston: Eviction Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json' })
		get_boston_evictions = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_evictions, this_script)

		doc.usage(get_boston_evictions, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' })

		boston_evictions = doc.entity('dat:agoncharova_lmckone#boston_evictions', {prov.model.PROV_LABEL:'Boston: Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_evictions, this_script)
		doc.wasGeneratedBy(boston_evictions, get_boston_evictions, endTime)
		doc.wasDerivedFrom(boston_evictions, resource, get_boston_evictions, get_boston_evictions, get_boston_evictions)
		repo.logout()

		return doc

boston_evictions.execute()
# boston_evictions.provenance()