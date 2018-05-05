import json
from shapely.geometry import shape, Point
import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid
import statistics as s

import pymongo
from bson.code import Code
import pprint
import pandas as pd
import numpy as np

class insert_correlations(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.boston_tracts_scores']
	writes = ['agoncharova_lmckone.correlations']

	@staticmethod
	def execute(trial = False):
		'''Performs some statistical analysis on our data'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection("correlations")
		repo.createCollection("correlations")

		#copy collections
		tracts = repo['agoncharova_lmckone.boston_tracts_scores']
		tracts_dict = [x for x in tracts.find()]
		print(tracts_dict[0].keys())

		#make a lil dataframe for ease of statistical analysis
		tract_properties = [{'tract':x['properties']['GEOID'],
		'evictions': x['properties']['evictions'], 
		'crimes': x['properties']['crimes'], 
		'businesses':x['properties']['businesses'], 
		'income':x['properties']['income'],
		'score':x['properties']['income']} for x in tracts.find()]

		df = pd.DataFrame(tract_properties)
		df = df[df['income']>0]

		quantiles = df.quantile([0.25, 0.5, 0.75, 1])

		#create data frames split on income quantiles to examine how
		#correlations change at different levels of income
		low_income = df.loc[df['income']<=float(quantiles['income'][0.25])]
		med_1_income = df.loc[(df['income']<float(quantiles['income'][0.5])) & (df['income']>float(quantiles['income'][0.25]))]
		med_2_income = df.loc[(df['income']<float(quantiles['income'][0.75])) & (df['income']>float(quantiles['income'][0.5]))]
		high_income = df.loc[df['income']>=float(quantiles['income'][0.75])]


		dict_to_insert = [{'businesses': {'evictions': {'low':low_income['evictions'].corr(low_income['businesses']),
		'lowmed':med_1_income['evictions'].corr(med_1_income['businesses']),
		'medhigh':med_2_income['evictions'].corr(med_2_income['businesses']),
		'high':high_income['evictions'].corr(high_income['businesses'])},
		'score':{'low':low_income['score'].corr(low_income['businesses']),
		'lowmed':med_1_income['score'].corr(med_1_income['businesses']),
		'medhigh':med_2_income['score'].corr(med_2_income['businesses']),
		'high':high_income['score'].corr(high_income['businesses'])}},
		'evictions':{'stability': {'low':low_income['score'].corr(low_income['evictions']),
		'lowmed':med_1_income['score'].corr(med_1_income['evictions']),
		'medhigh':med_2_income['score'].corr(med_2_income['evictions']),
		'high':high_income['score'].corr(high_income['evictions'])}}}]


		repo['agoncharova_lmckone.correlations'].insert_many(dict_to_insert)
		repo['agoncharova_lmckone.correlations'].metadata({'complete':True})


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

		this_script = doc.agent('alg:agoncharova_lmckone#insert_correlations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_correlations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		tracts_input = doc.entity('dat:agoncharova_lmckone.boston_tracts_scores',
			{prov.model.PROV_LABEL:'Census Tract Information',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		output = doc.entity('dat:agoncharova_lmckone.correlations',
			{prov.model.PROV_LABEL:'Correlations', prov.model.PROV_TYPE:'ont:DataSet'})


		doc.wasAssociatedWith(get_correlations, this_script)

		doc.usage(get_correlations, tracts_input, startTime, None,
          {prov.model.PROV_TYPE:'ont:Computation'}
          )

		doc.wasAttributedTo(output, this_script)
		doc.wasGeneratedBy(output, get_correlations, endTime)
		doc.wasDerivedFrom(output, tracts_input, get_correlations, get_correlations, get_correlations)
		

		repo.logout()
				  
		return doc


insert_correlations.execute()
