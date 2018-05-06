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

class statistical_analysis(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.boston_tracts_all_counts', 'agoncharova_lmckone.stability_score']
	writes = []

	@staticmethod
	def execute(trial = False):
		'''Performs some statistical analysis on our data'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		#copy collections
		stability_score = repo['agoncharova_lmckone.stability_score']
		boston_tracts_all_counts = repo['agoncharova_lmckone.boston_tracts_all_counts']

		#make a lil dataframe for ease of statistical analysis
		scores = [{'tract': x['Tract'], 'score': x['stability']} for x in stability_score.find()]
		properties = [{'tract':x['properties']['GEOID'],
		'evictions': x['properties']['evictions'], 
		'crimes': x['properties']['crimes'], 
		'businesses':x['properties']['businesses'], 
		'income':x['properties']['income']} for x in boston_tracts_all_counts.find()]
		print("inserting to scores into properties dictionaries")
		for x in properties:
			matches = [score['score'] for score in scores if score['tract'] == x['tract']]
			if matches:
				x['score'] = float(matches[0])
			else:
				x['score'] = 0
		print("scores inserted")
		properties_df = pd.DataFrame(properties)
		properties_df = properties_df[properties_df['income']>0]

		#get quantiles of each property
		quantiles = properties_df.quantile([0.25, 0.5, 0.75, 1])

		#create data frames split on income quantiles to examine how
		#correlations and other summary statistics change at different
		#levels of income
		low_income = properties_df.loc[properties_df['income']<=float(quantiles['income'][0.25])]
		med_1_income = properties_df.loc[(properties_df['income']<float(quantiles['income'][0.5])) & (properties_df['income']>float(quantiles['income'][0.25]))]
		med_2_income = properties_df.loc[(properties_df['income']<float(quantiles['income'][0.75])) & (properties_df['income']>float(quantiles['income'][0.5]))]
		high_income = properties_df.loc[properties_df['income']>=float(quantiles['income'][0.75])]

		#correlation between businesses and evictions by median income of tract
		print(low_income['evictions'].corr(low_income['businesses']))
		print(med_1_income['evictions'].corr(med_1_income['businesses']))
		print(med_2_income['evictions'].corr(med_2_income['businesses']))
		print(high_income['evictions'].corr(high_income['businesses']))

		#correlation between businesses and stability by median income of tract
		print(low_income['score'].corr(low_income['businesses']))
		print(med_1_income['score'].corr(med_1_income['businesses']))
		print(med_2_income['score'].corr(med_2_income['businesses']))
		print(high_income['score'].corr(high_income['businesses']))

		#summary statistics for low income tracts
		print("mean number of businesses in low income tracts")
		print(s.mean(low_income['businesses']))
		print("mean number of evictions in low income tracts")
		print(s.mean(low_income['evictions']))

		#summary statistics for med 1 income tracts
		print("mean number of businesses in med 1 income tracts")
		print(s.mean(med_1_income['businesses']))
		print("mean number of evictions in med 1 income tracts")
		print(s.mean(med_1_income['evictions']))

		#summary statistics for med 2 income tracts
		print("mean number of businesses in med 2 income tracts")
		print(s.mean(med_2_income['businesses']))
		print("mean number of evictions in med 2 income tracts")
		print(s.mean(med_2_income['evictions']))

		#summary statistics for high income tracts
		print("mean number of businesses in high income tracts")
		print(s.mean(high_income['businesses']))
		print("mean number of evictions in high income tracts")
		print(s.mean(high_income['evictions']))



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

		this_script = doc.agent('alg:agoncharova_lmckone#statistical_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_statistical_analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		scores_input = doc.entity('dat:agoncharova_lmckone.stability_score',
			{prov.model.PROV_LABEL:'Stability scores',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		tracts_input = doc.entity('dat:agoncharova_lmckone.boston_tracts_all_counts',
			{prov.model.PROV_LABEL:'Census Tract Information',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		doc.wasAssociatedWith(get_statistical_analysis, this_script)

		doc.usage(get_statistical_analysis, scores_input, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'}
		          )

		doc.usage(get_statistical_analysis, tracts_input, startTime, None,
          {prov.model.PROV_TYPE:'ont:Computation'}
          )

		repo.logout()
				  
		return doc


#statistical_analysis.execute()



#url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_25025_tabblock10.zip'
#response = ur.urlopen(url)
#data = json.load(response)
#print(data)