import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import pandas as pd
import time

class getCrimes(dml.Algorithm):
	contributor = 'janellc_rstiffel_yash'
	reads = []
	writes = ['janellc_rstiffel_yash.crimesData']

	@staticmethod
	def execute(trial=False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')

		repo.dropCollection("crimesData")
		repo.createCollection("crimesData")

		n = 0
		df = pd.DataFrame()
		startTime = time.time()
		while True:
			url = 'https://data.boston.gov/api/3/action/datastore_search?offset='+str(n)+'&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'  
			response = requests.get(url)
			r = response.json()['result']['records']
			s = json.dumps(r, sort_keys=True, indent=2)
			df = pd.concat([df, pd.read_json(s)], axis = 0)
			repo['janellc_rstiffel_yash.crimesData'].insert_many(r)

			if trial == True:
				if n == 2000:
					break
			if n == 200000:
				break
			n=n+100 #offset

		repo['janellc_rstiffel_yash.crimesData'].metadata({'complete':True})
		print(repo['janellc_rstiffel_yash.crimesData'].metadata())

		repo.logout()
		endTime = time.time()
		#print('Time taken to parse the file: ' + str(endTime - startTime))
		#df.reset_index(drop=True).to_csv('crime_data.csv')
		return {"start":startTime, "end":endTime}


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/janellc_rstiffel_yash#') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/janellc_rstiffel_yash#') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		doc.add_namespace('cri', 'https://data.boston.gov/')


		this_script = doc.agent('alg:getCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('cri:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_crimes, this_script)

		doc.usage(get_crimes, resource, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Retrieval',
		          'ont:Query':'api/3/action/datastore_search?offset=$&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'
		          }
		          )

		crimes_dat = doc.entity('dat:crimesData', {prov.model.PROV_LABEL:'Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crimes_dat, this_script)
		doc.wasGeneratedBy(crimes_dat, get_crimes, endTime)
		doc.wasDerivedFrom(crimes_dat, resource, get_crimes, get_crimes, get_crimes)


		repo.logout()
		          
		return doc

#getCrimes.execute(True)
#doc = getCrimes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
## eof