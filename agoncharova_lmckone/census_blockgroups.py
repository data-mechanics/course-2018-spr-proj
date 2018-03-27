from census import Census
from us import states
import requests
import json
import urllib.request as ur
import dml
import prov.model
import datetime
import uuid

class census_blockgroups(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.census_blockgroups']

	@staticmethod
	def execute(trial = False):
		'''Retrieve Boston Assessing Data from 2014-2017'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		repo.dropCollection("census_blockgroups")
		repo.createCollection("census_blockgroups")


		c = Census("d07b9c8b06ff352d4f8217ddc4f15737fc6c867c", year=2015)
		#get median income in 2015 for all blockgroups in Massachusetts
		data = c.acs5.state_county_tract('B06011_001E', states.MA.fips, '025', Census.ALL)
		print(type(data))

		repo['agoncharova_lmckone.census_blockgroups'].insert_many(data)
		repo['agoncharova_lmckone.census_blockgroups'].metadata({'complete':True})

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

		doc.add_namespace('bdp', 'https://data.boston.gov/export/')

		this_script = doc.agent('alg:agoncharova_lmckone#census_blockgroups', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('sfdp:93gi-sfd2', {'prov:label':'San Francisco Data Portal Evictions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_census_blockgroups = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_census_blockgroups, this_script)

		doc.usage(get_census_blockgroups, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval'}
				  )

		census_blockgroups = doc.entity('dat:agoncharova_lmckone#census_blockgroups', {prov.model.PROV_LABEL:'Income by Blockgroup', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(census_blockgroups, this_script)
		doc.wasGeneratedBy(census_blockgroups, get_census_blockgroups, endTime)
		doc.wasDerivedFrom(census_blockgroups, resource, get_census_blockgroups, get_census_blockgroups, get_census_blockgroups)
		repo.logout()
				  
		return doc


census_blockgroups.execute()



#url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_25025_tabblock10.zip'
#response = ur.urlopen(url)
#data = json.load(response)
#print(data)


