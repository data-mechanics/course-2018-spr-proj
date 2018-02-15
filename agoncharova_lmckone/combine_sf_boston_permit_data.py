import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import pymongo
import re
import uuid

class combine_sf_boston_permit_data(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = [ 'agoncharova_lmckone.sf_permits', 'agoncharova_lmckone.boston_permits' ]
	writes = ['agoncharova_lmckone.boston_sf_permits']
	
	pp = pprint.PrettyPrinter(indent=2)

	def combine_columns(arr, delim, data_item):
		'''
		Grabs values that have keys that are in arr for a data_item and combines 
		them using delim as the delimeter.
		'''
		cn = combine_sf_boston_permit_data # 'cn' stands for 'classname'
		result = [cn.get_if_exists(data_item, key) for key in arr]	
		return delim.join(result)

	def get_if_exists(data_item, field_name):
		'''
		Checks if a data_item contains a field_name as a key. 
		Returns its value if it does, and if not, an empty string.
		'''
		if field_name in data_item:
			return data_item[field_name]
		else:
			return ""
	
	def process_sf_item(sf_item):
		'''
		Look at the SF data item and reassign its fields.
		Save into new collection.
		'''
		cn = combine_sf_boston_permit_data # 'cn' stands for 'classname'

		new_item = {}
		new_item["source"] = "sf"
		new_item["city"] = "SF"
		new_item["state"] = "CA"
		new_item["_id"] = sf_item["_id"]
		new_item["permit_number"] = sf_item["permit_number"]
		new_item["type"] = sf_item["permit_type"]		
		new_item["definition"] = sf_item["permit_type_definition"]
		new_item["comments"] = cn.get_if_exists(sf_item, "description")
		new_item["issued_date"] = cn.get_if_exists(sf_item, "issued_date")
		new_item["expiration_date"] =  cn.get_if_exists(sf_item, "permit_expiration_date")
		new_item["valuation"] = cn.get_if_exists(sf_item, "estimated_cost")
		new_item["status"] = sf_item["status"]
		new_item["current_use"] = cn.get_if_exists(sf_item, "existing_use")
		keys_to_combine = ["street_number", "street_name", "street_suffix"]
		new_item["street"] = cn.combine_columns(keys_to_combine, " ", sf_item)		
		new_item["zipcode"] = cn.get_if_exists(sf_item, "zipcode")
		new_item["location"] =  cn.get_if_exists(sf_item, "location")
		keys_to_combine = ["block", "lot"]
		new_item["property_id"] = cn.combine_columns(keys_to_combine, "-", sf_item)
		return new_item
	
	def process_boston_item(boston_item):
		'''
		Look at the Boston data item and reassign its fields.
		Save into new collection.
		'''
		new_item = {}

		new_item["source"] = "boston"
		new_item["state"] = "MA"
		new_item["_id"] = boston_item["_id"]
		new_item["permit_number"] = boston_item["PermitNumber"]
		new_item["type"] = boston_item["WORKTYPE"]
		new_item["definition"] = boston_item["PermitTypeDescr"]
		new_item["comments"] = boston_item["Comments"]
		new_item["issued_date"] = boston_item["ISSUED_DATE"]
		new_item["expiration_date"] = boston_item["EXPIRATION_DATE"]
		new_item["valuation"] = boston_item["DECLARED_VALUATION"]
		new_item["status"] = boston_item["STATUS"]
		new_item["current_use"] = boston_item["OCCUPANCYTYPE"]
		new_item["street"] = boston_item["ADDRESS"]		
		new_item["city"] = boston_item["CITY"]
		new_item["zipcode"] = boston_item["ZIP"]
		new_item["location"] = boston_item["Location"]
		new_item["property_id"] = boston_item["Property_ID"]
		return new_item


	@staticmethod
	def execute(trial = False):
		''' Retrieve SF and Boston Permit data from
		MongoDB collections and combine it into a single dataset'''
		cn = combine_sf_boston_permit_data # 'cn' stands for 'classname'
		startTime = datetime.datetime.now()
		new_coll_name = "agoncharova_lmckone.boston_sf_permits"

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		repo.dropCollection( new_coll_name )
		repo.createCollection( new_coll_name )
		
		boston_permit_coll = repo.agoncharova_lmckone.boston_permits
		sf_permit_coll = repo.agoncharova_lmckone.sf_permits

		# just get all of the data lol
		boston_data = boston_permit_coll.find()
		sf_data = sf_permit_coll.find()
		
		print("Boston data items count: " + str(boston_data.count()))
		print("SF data items count: " + str(sf_data.count()))
		for item in boston_data:
			new_item = cn.process_boston_item(item)
			repo[new_coll_name].insert(new_item)
		
		for item in sf_data:
			new_item = cn.process_sf_item(item)
			# save to db
			repo[new_coll_name].insert(new_item)
			
		print("After processing, both Boston and SF permits, total number of data is: ")	
		print(repo[new_coll_name].count())
		return 0

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo

		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.boston.gov/export/')
		doc.add_namespace('sfdp', 'https://datasf.org/opendata/')

		this_script = doc.agent('alg:agoncharova_lmckone#boston_sf_permits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:6ddcd912-32a0-43df-9908-63574f8c7e77', {'prov:label':'Combined Boston and SF Permit data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		
		boston_sf_permits = doc.entity('dat:agoncharova_lmckone#boston_sf_permits', {prov.model.PROV_LABEL:'Combined Boston and SF Permit data', prov.model.PROV_TYPE:'ont:DataSet'})
		get_boston_sf_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_sf_permits, this_script)
		doc.usage(get_boston_sf_permits, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )
		
		get_boston_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_permits, this_script)
		doc.usage(get_boston_permits, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )

		get_sf_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_sf_permits, this_script)
		doc.usage(get_sf_permits, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' } )

		
		doc.wasAttributedTo(boston_sf_permits, this_script)
		doc.wasGeneratedBy(get_boston_sf_permits, get_boston_permits, endTime)
		doc.wasDerivedFrom(boston_sf_permits, resource, get_boston_permits, get_boston_permits, get_boston_permits)
		doc.wasDerivedFrom(boston_sf_permits, resource, get_sf_permits, get_sf_permits, get_sf_permits)
		repo.logout()
		      
		return doc

combine_sf_boston_permit_data.execute()
combine_sf_boston_permit_data.provenance()

