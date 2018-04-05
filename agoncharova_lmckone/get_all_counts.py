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

class get_all_counts(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.income_tracts', 'agoncharova_lmckone.boston_tract_counts']
	writes = ['agoncharova_lmckone.boston_tracts_all_counts']

	@staticmethod
	def execute(trial = False):
		'''Inserts income into the repo'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection("boston_tracts_all_counts")
		repo.createCollection("boston_tracts_all_counts")

		boston_tract_counts = repo['agoncharova_lmckone.boston_tract_counts']
		income_tracts = repo['agoncharova_lmckone.income_tracts']
		#concatenate state county and tract codes into FIPS and associates with income
		incomes = [{'GEOID': x['state']+x['county']+x['tract'],'income': x['B06011_001E']} for x in income_tracts.find()]

		###eventually maybe include new construction permits here? could be interested but format is unreliable,
		###also would need to filter by year probably.



		#copy collection into a fresh list that we will manipulate and reinsert into the db
		boston_tracts_all_counts = [x for x in boston_tract_counts.find()]

		#insert incomes into the census tracts collection, in 'properties'
		print("inserting income count...")
		for feature in boston_tracts_all_counts:
			matches = [income['income'] for income in incomes if income['GEOID'] == feature['properties']['GEOID'] and income['income'] is not None]
			if matches:
				feature['properties']['income'] = float(matches[0])
			else:
				feature['properties']['income'] = 0
		print("incomes inserted")


		#write to the db

		repo['agoncharova_lmckone.boston_tracts_all_counts'].insert_many(boston_tracts_all_counts)
		repo['agoncharova_lmckone.boston_tracts_all_counts'].metadata({'complete':True})

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

		this_script = doc.agent('alg:agoncharova_lmckone#get_all_counts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_all_counts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		income_input = doc.entity('dat:agoncharova_lmckone.income_tracts',
			{prov.model.PROV_LABEL:'Income',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		tracts_input = doc.entity('dat:agoncharova_lmckone.boston_tract_counts',
			{prov.model.PROV_LABEL:'Census Tract Information',prov.model.PROV_TYPE:'ont:DataSet', 'ont:Query': '.find()'})

		output = doc.entity('dat:agoncharova_lmckone.boston_tracts_all_counts',
			{prov.model.PROV_LABEL:'Count of evictions, crimes, and income in each census tract', prov.model.PROV_TYPE:'ont:DataSet'})

		doc.wasAssociatedWith(get_all_counts, this_script)

		doc.usage(get_all_counts, income_input, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'}
		          )

		doc.usage(get_all_counts, tracts_input, startTime, None,
          {prov.model.PROV_TYPE:'ont:Computation'}
          )

		doc.wasAttributedTo(output, this_script)
		doc.wasGeneratedBy(output, get_all_counts, endTime)
		doc.wasDerivedFrom(output, income_input, get_all_counts, get_all_counts, get_all_counts)
		doc.wasDerivedFrom(output, tracts_input, get_all_counts, get_all_counts, get_all_counts)
		
		repo.logout()
				  
		return doc


#get_all_counts.execute()



#url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_25025_tabblock10.zip'
#response = ur.urlopen(url)
#data = json.load(response)
#print(data)