import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import uuid

class foursquare_retrieve(dml.Algorithm):

	# TODO: Is is imperative to add @staticmethod annotations to the methods?
	
	pp = pprint.PrettyPrinter(indent=2)
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.foursquare_boston', 'agoncharova_lmckone.foursquare_sf']

	# request data
	CLIENT_SECRET = 'BI1UF1MR2D5LFJPZT35BABSEU5JQNDGNMMZEBZDKZ4D4ZFNM'
	CLIENT_ID = 'OBXTQASYLDVZVYFNR4HIKVSHWXV1VT0CZYLHFJVSG0D4ANGX'
	CATEGORY_ID = '4bf58dd8d48988d124941735' # 'Offices' category
	api_version = '20180201' # use Feb 1, 2018
	radius = 1500 # in meters 
	# url string to be later `format`ted
	url_string = (
		"https://api.foursquare.com/v2/venues/search"
		"?client_id={0}"
		"&client_secret={1}"
		"&categoryId={2}"
		"&ll={3}"
		"&v={4}"
		"&radius={5}"
		"&limit=100"
	)
	@staticmethod
	def get_coords(city):
		'''
		Returns a set of long and lat coords for use with 
		the Foursquare API, since there is a 50 item limit per query.
		Used by get_data_by_city that actually queries the API.
		'''
		coords = []
		if(city == 'SF'):
			coords = [[37.80, -122.51], [37.70, -122.51], [37.80, -122.40], [37.70, -122.40]]
			for lon in range(0, 12):	
				for lat in range(0, 11):			
					y = 37.70 + (lat/100.0)
					x = -122.51 + (lon/100.0)
					coords.append([float("{0:.2f}".format(y)), float("{0:.2f}".format(x))])
		if(city == 'Boston'):
			coords = [[42.40, -71.19], [42.30, -71.19], [42.40, -71.02], [42.30, -71.02]]
			for lon in range(0, 11):	
				for lat in range(0, 18):			
					y = 42.30 + (lat/100.0)
					x = -71.19 + (lon/100.0)
					coords.append([float("{0:.2f}".format(y)), float("{0:.2f}".format(x))])
		return coords

	@staticmethod
	def construct_set_of_queries(city):
		'''
		Returns an arrary of string URL queries, where
		the only difference are the coordinates
		''' 
		fr = foursquare_retrieve
		coords = fr.get_coords(city)
		set_of_urls = []
		print("Constructing set of URLs for: " + city)
		for coord in coords: 
			coords = '{0},{1}'.format(coord[0], coord[1])
			url = foursquare_retrieve.url_string.format(
				foursquare_retrieve.CLIENT_ID, 
				foursquare_retrieve.CLIENT_SECRET, 
				foursquare_retrieve.CATEGORY_ID, 
				coords, 
				foursquare_retrieve.api_version, 
				foursquare_retrieve.radius)
			set_of_urls.append(url)
		return set_of_urls


	@staticmethod
	def get_data_by_city(city):
		''' 
		Constructs and issues a request depending on the 
		`city` param passed in. 
		'''
		fr = foursquare_retrieve
		pp = fr.pp				
		all_data = {}
		# get a set of url queries
		set_of_queries = fr.construct_set_of_queries(city)
		for query in set_of_queries:
			# make the request
			response = urllib.request.urlopen(query)
			data = json.load(response)['response']['venues']
			for place in data: 
				all_data[place['id']] = place	
		# do this to format the data
		for_mongo = []
		for item in all_data: 
			for_mongo.append(all_data[item])
		return for_mongo

	# The @staticmethod decorator makes a method such that it 
	# can be called from an uninstantiated class object
	@staticmethod
	def execute(trial = False):
		''' 
		Retrives business data using the Foursquare API for Boston and SF
		and saves to a database
		'''
		fr = foursquare_retrieve
		pp = fr.pp
		# names of db and collections 
		db_name = 'agoncharova_lmckone'
		sf_coll = 'agoncharova_lmckone.sf_businesses'
		boston_coll = 'agoncharova_lmckone.boston_businesses'
		
		# setup
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		
		# get business data from Foursquare and save to DB collections {sf_businesses, boston_businesses}
		response = fr.get_data_by_city("SF")
		print("Got the following number of businesses in SF:")
		print(len(response))
		repo.dropCollection( sf_coll )
		repo.createCollection( sf_coll )
		repo[ sf_coll ].insert_many(response)
		repo[ sf_coll ].metadata( {'complete':True} )
		print("Saved SF data")
		print(repo[ sf_coll ].metadata())
		
		response = fr.get_data_by_city("Boston")
		print("Got the following number of businesses in Boston:")		
		print(len(response))
		repo.dropCollection( boston_coll )
		repo.createCollection( boston_coll )
		repo[ boston_coll ].insert_many(response)
		repo[ boston_coll ].metadata( {'complete':True} )
		print("Saved Boston data")
		print(repo[ boston_coll ].metadata())
		

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''
		# shorten class name
		fr = foursquare_retrieve
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		# custom data sources
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('4sq', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('sfdp', 'https://datasf.org/opendata/')

		this_script = doc.agent('alg:agoncharova_lmckone#foursquare_retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		# TODO: Is the value after bdp below a random id?
		resource = doc.entity('bdp:40e2-897e', {'prov:label':'Foursquare, Office Data for Boston and SF', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		#	separate by SF and Boston data
		get_sf = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_sf, this_script)
		doc.wasAssociatedWith(get_boston, this_script)
		# TODO: How do we format the complex set of queries to the API?
		# SF query
		sf_queries = fr.construct_set_of_queries("SF")
		doc.usage(get_sf, resource, startTime, None,
							{prov.model.PROV_TYPE:'ont:Retrieval',
							'ont:Query': "|".join(sf_queries)
							}
							)
		# Boston query
		boston_queries = fr.construct_set_of_queries("Boston")
		doc.usage(get_boston, resource, startTime, None,
							{prov.model.PROV_TYPE:'ont:Retrieval',
							'ont:Query': "|".join(boston_queries)
							}
							)
		
		sf_businesses = doc.entity('dat:agoncharova_lmckone#sf_businesses', {prov.model.PROV_LABEL:'SF Businesses', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(sf_businesses, this_script)
		doc.wasGeneratedBy(sf_businesses, get_sf, endTime)
		doc.wasDerivedFrom(sf_businesses, resource, get_sf, get_sf, get_sf)

		boston_businesses = doc.entity('dat:alice_bob#boston_businesses', {prov.model.PROV_LABEL:'Boston Businesses', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_businesses, this_script)
		doc.wasGeneratedBy(boston_businesses, get_boston, endTime)
		doc.wasDerivedFrom(boston_businesses, resource, get_boston, get_boston, get_boston)

		repo.logout()
		        
		return doc

# foursquare_retrieve.execute()
# foursquare_retrieve.provenance()

