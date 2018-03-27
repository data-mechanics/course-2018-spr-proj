from census import Census
from us import states
import requests
import json
import urllib.request as ur
import dml
import prov.model
import datetime
import uuid

class income_tracts(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.income_tracts']

	@staticmethod
	def execute(trial = False):
		'''Retrieve Boston Assessing Data from 2014-2017'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection("income_tracts")
		repo.createCollection("income_tracts")


		c = Census("d07b9c8b06ff352d4f8217ddc4f15737fc6c867c", year=2015)
		#get median income in 2015 for all census tracts in Massachusetts
		data = c.acs5.state_county_tract('B06011_001E', states.MA.fips, '025', Census.ALL)
		print(type(data))

		repo['agoncharova_lmckone.income_tracts'].insert_many(data)
		repo['agoncharova_lmckone.income_tracts'].metadata({'complete':True})

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

		doc.add_namespace('census', 'https://api.census.gov/data/')

		this_script = doc.agent('alg:agoncharova_lmckone#income_tracts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('census:B06011_001E', {'prov:label':'Census API income by census tract', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_income_tracts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_income_tracts, this_script)

		doc.usage(get_income_tracts, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval'}
				  )

		income_tracts = doc.entity('dat:agoncharova_lmckone#income_tracts', {prov.model.PROV_LABEL:'Income by Census Tract', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(income_tracts, this_script)
		doc.wasGeneratedBy(income_tracts, get_income_tracts, endTime)
		doc.wasDerivedFrom(income_tracts, resource, get_income_tracts, get_income_tracts, get_income_tracts)
		repo.logout()
				  
		return doc


income_tracts.execute()



#url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_25025_tabblock10.zip'
#response = ur.urlopen(url)
#data = json.load(response)
#print(data)


