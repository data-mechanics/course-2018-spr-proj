import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import pymongo
import uuid
import re
import functools

'''
We count how many businesses and permits are associated with a
certain zipcode
'''
class count_activity(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = [ 'agoncharova_lmckone.sf_businesses', 
		'agoncharova_lmckone.boston_businesses',
		'agoncharova_lmckone.boston_sf_permits' ]
	writes = []

	pp = pprint.PrettyPrinter(indent=2)

	def get_4sq_data(city = "SF"):
		'''
	 	Tap into MongoDB to return all data in a collection for
	 	a particular city, SF by default.
		'''
		client = pymongo.MongoClient()
		db = client.repo
		collection = db.agoncharova_lmckone.sf_businesses
		result = []
		if(city == "Boston"):
			# find({'location.postalCode': {$regex:'02(1|2)'}})			
			collection = db.agoncharova_lmckone.boston_businesses
			# get all the data for boston area ZZIPcodes
			regx = re.compile("02(1|2)", re.IGNORECASE) # returns 735 businesses
			for item in collection.find({"location.postalCode": {"$regex": regx}}):
				result.append(item)
		else:
			collection = db.agoncharova_lmckone.sf_businesses
			for item in collection.find({}):
				result.append(item)
		return result

	def filter_4sq_data_by_office_type(data):
		'''
		Returns data that contains only "Coworking Space", or "Tech Startup", or
		"Conference Room"
		# for boston area, returns 167 pts
		'''
		fa = count_activity
		result = []
		filter_by = ["Coworking Space", "Tech Startup", "Conference Room", "IT Services"] 
		for item in data:
			cats = item["categories"]
			for cat in cats:
				if cat["shortName"] in filter_by:
					result.append(item)
		return result

	@staticmethod
	def filter_permits():
		'''
		Filters permit data for Boston and permit type = 1 (SF permit type)
		'''
		fa = count_activity

		# get permit data for boston from the unified database
		client = pymongo.MongoClient()
		db = client.repo
		all_permits = db["agoncharova_lmckone.boston_sf_permits"]

		data =  all_permits.find( { "$or": [ {"source":"boston"}, {"type":"1"} ] } )

		return data

	def count(zip_codes):
		return functools.reduce(count_activity.reducer, map(lambda zip_code: dict( [ [zip_code, 1] ] ), zip_codes))

	def reducer(i, j):
		for k in j: i[k] = i.get(k, 0) + j.get(k, 0)
		return i

	def get_list_of_zipcodes_4sq(data):
		return [item['location']['postalCode'] for item in data]

	def get_list_of_permit_zipcodes(permits):
		return [item['zipcode'] for item in permits]

	def combine_counts(permits, boston, sf):
		'''
		Traverse through boston and sf business zipcode count
		and return a combined object of the form {'zipcode': (num_bussinesses, num_permits)}
		'''
		all_zipcodes = {}
		for zipcode, count in permits.items():
			if zipcode not in all_zipcodes:
				all_zipcodes[zipcode] = [0, count]
			if (zipcode in boston):
				all_zipcodes[zipcode][0] = boston[zipcode]
			elif (zipcode in sf):
				all_zipcodes[zipcode][0] = sf[zipcode]
		return all_zipcodes


	@staticmethod
	def execute():
		fa = count_activity
		boston_4sq = fa.get_4sq_data("Boston")
		sf_4sq = fa.get_4sq_data() # don't pass in an arg cause SF by default

		filtered_boston_data = fa.filter_4sq_data_by_office_type(boston_4sq)
		filtered_sf_data = fa.filter_4sq_data_by_office_type(sf_4sq)

		permits = fa.filter_permits()
		permits_zipcodes = fa.get_list_of_permit_zipcodes(permits[:15])
		# get counts of permits
		count_of_permit_zipcodes = fa.count(permits_zipcodes)

		# get all business zipcode counts for boston AND sf
		boston_zipcodes = fa.get_list_of_zipcodes_4sq(filtered_boston_data)
		boston_counts = fa.count(boston_zipcodes)
		print(boston_counts)
		print("\n")

		sf_zipcodes = fa.get_list_of_zipcodes_4sq(filtered_sf_data)
		sf_counts = fa.count(sf_zipcodes)
		print(sf_counts)
		print("\n")

		combined_counts = fa.combine_counts(count_of_permit_zipcodes, boston_counts, sf_counts)
		# TODO: do we want to save these counts?

		return 0

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('4sq', 'https://developer.foursquare.com/places-api')

		this_script = doc.agent('alg:agoncharova_lmckone#count_activity', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('dat:d7b9-4064', {'prov:label':'Count Activity by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		
		count_activity = doc.entity('dat:agoncharova_lmckone#count_activity', {prov.model.PROV_LABEL:'Count Activity by Zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
		
		get_count_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_count_activity, this_script)
		doc.usage(get_count_activity, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )

		get_4sq_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_4sq_data, this_script)
		doc.usage(get_4sq_data, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )

		get_boston_sf_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_sf_permits, this_script)
		doc.usage(get_boston_sf_permits, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )

		
		doc.wasAttributedTo(count_activity, this_script)
		doc.wasGeneratedBy(count_activity, get_count_activity, endTime)
		doc.wasDerivedFrom(count_activity, resource, get_boston_sf_permits, get_boston_sf_permits, get_boston_sf_permits)
		doc.wasDerivedFrom(count_activity, resource, get_4sq_data, get_4sq_data, get_4sq_data)
		repo.logout()
		return doc

count_activity.provenance()
# count_activity.execute()
