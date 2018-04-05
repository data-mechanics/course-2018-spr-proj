from sklearn import preprocessing
from scipy.stats.stats import pearsonr
import pandas as pd
import prov.model
import datetime
import uuid
import dml
import sys
sys.path.append("/Users/lubovmckone/course-2018-spr-proj/agoncharova_lmckone/z3/build/python/")
from z3 import *

class optimal_score(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.stability_score']
	writes = ['agoncharova_lmckone.optimal_score']

	# HELPER
	@staticmethod
	def get_stability_scores_from_repo():
		'''
		Gets and returns the data from the stability_score collection.
		'''
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		stability_score_collection = repo['agoncharova_lmckone.stability_score']

		stability_scores_cursor = stability_score_collection.find()
		stability_scores_arr = []
		for stability_score_object in stability_scores_cursor:
			stability_scores_arr.append(stability_score_object)
		repo.logout()
		return stability_scores_arr

	@staticmethod
	def explore_business_data(all_data):
		'''
		Normalize business data and compute correlation coefficient 
		between the number 
		'''
		# normalize the business scores for each entry and look whether they are correlated
		# inspiration: http://sebastianraschka.com/Articles/2014_about_feature_scaling.html#about-standardization
		df = pd.DataFrame(all_data)

		std_scale = preprocessing.StandardScaler().fit(df[['businesses']])
		df_std = std_scale.transform(df[['businesses']])
		
		minmax_scale = preprocessing.MinMaxScaler().fit(df[['businesses']])
		df_minmax = minmax_scale.transform(df[['businesses']])

		stability_list = list(df['stability'].as_matrix())
		print(len(stability_list))
		df_minmax_formatted = [x[0] for x in df_minmax]
		print(len(df_minmax_formatted))
		# find correlation between the normalized business scores and the optimal score
		corr = pearsonr(stability_list, df_minmax_formatted)  # corr = 0.10446045881042658, 2-tailed p-val = 0.13704210138510017 
		# non zero correlation of 0.1 obviously means it is CORRELATED
		return corr[0]

	@staticmethod
	def compute_optimal_num_businesses(all_data):
		'''
		High number of evictions and crimes lead to a higher score, so 
		we want to lower the score?
		z3 solver allows us to solve proprietary equations with
		various constraints.
		'''
		# setup
		df = pd.DataFrame(all_data)
		# isolate and format the vars
		tracts = list(df['Tract'].as_matrix())
		businesses = list(df['businesses'].as_matrix())
		stability = list(df['stability'].as_matrix())
		additional_businesses = [9999]*len(businesses) # 204 entries
		results = []
		# let's optimize
		for i in range(204):
			S = Optimize()
			new = Real('new'+str(i))
			num_businesses = Int('num_businesses'+str(i))
			S.add(new > 0) # new stability score has to be positive
			S.minimize(new)
			if(stability[i] > 0.5):
				S.add(num_businesses <= 5) # we can't add more than 5 businesses
				S.add(num_businesses == (stability[i] - new)/ 0.2)
			if(stability[i] < 0.5 and stability[i] > 0.3):
				S.add(num_businesses <= 3) # we can't add more than 3 businesses
				S.add(num_businesses == (stability[i] - new)/ 0.1)
			S.check()
			model = S.model()
			new_score = model[new].as_decimal(5)[:7]
			addl_businesses = model[num_businesses].as_long()
			result = {"Tract": tracts[i],
								"new_optimal_score": new_score,
								"stability_score": str(stability[i]),
								"addl_businesses": str(addl_businesses),
								"businesses": str(businesses[i]) }
			results.append(result)
		return results

	@staticmethod
	def execute(trial=False):
		'''
		Incorporate the number of businesses into the tract stability score 
		by first normalizing it in construct_business_score.	
		Run an SMT solver using z3 library to see how many businesses could be added
		to a tract in order to improve stability score.
		'''
		startTime = datetime.datetime.now()
		
		this = optimal_score		

		all_data = this.get_stability_scores_from_repo()
		corr = this.explore_business_data(all_data)
		print("correlation between businesses and stability score is: " + str(corr))
		results = this.compute_optimal_num_businesses(all_data)

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		optimal_score_collection = repo['agoncharova_lmckone.optimal_score']

		repo.dropCollection('optimal_score')
		repo.createCollection('optimal_score')
		repo['agoncharova_lmckone.optimal_score'].insert_many(results)
		repo.logout()
		print("ran SMT solver and saved data to optimal_score collection")
		return {"start": startTime, "end":  datetime.datetime.now()}

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

		this_agent = doc.agent('alg:agoncharova_lmckone#optimal_score',
		                      	{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		this_entity = doc.entity('dat:agoncharova_lmckone#optimal_score',
                            {prov.model.PROV_LABEL: 'Optimal Score', prov.model.PROV_TYPE: 'ont:DataSet'})
		
		optimal_score_resource = doc.entity('dat:agoncharova_lmckone#optimal_score',
		                  {prov.model.PROV_LABEL: 'Optimal Score', prov.model.PROV_TYPE: 'ont:DataSet'})

		get_optimal_score = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.usage(get_optimal_score, optimal_score_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
		
		doc.wasAssociatedWith(get_optimal_score, this_agent)
		doc.wasAttributedTo(this_entity, this_agent)
		doc.wasGeneratedBy(this_entity, get_optimal_score, endTime)
		doc.wasDerivedFrom(this_entity, optimal_score_resource, get_optimal_score, get_optimal_score, get_optimal_score)

		repo.logout()

		return doc

# optimal_score.execute()
# optimal_score.provenance()
