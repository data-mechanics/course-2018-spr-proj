import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import uuid

class foursquare_retrieve(dml.Algorithm):
	
	pp = pprint.PrettyPrinter(indent=2)
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.foursquare_boston', 'agoncharova_lmckone.foursquare_sf']

	@staticmethod
	def get_data_by_city(city):
		''' 
		Constructs and issues a request depending on the 
		`city` param passed in. 
		'''
		pp = foursquare_retrieve.pp
		CLIENT_SECRET = 'HC0XDTTX1XIKWNK0N3G5G3F2HUZKY4MWAGHVYQOIX1TKG4WQ'
		CLIENT_ID = 'OBXTQASYLDVZVYFNR4HIKVSHWXV1VT0CZYLHFJVSG0D4ANGX'
		CATEGORY_ID = '4bf58dd8d48988d124941735' # 'Offices' category
		api_version = '20180201' # use Feb 1, 2018
		radius = 1500 # in meters 
		coords = foursquare_retrieve.get_coords(city)
		all_data = {}
		for coord in coords: 
			coords = '{0},{1}'.format(coord[0], coord[1])
			print(coords)
			url = (
				"https://api.foursquare.com/v2/venues/search"
				"?client_id={0}"
				"&client_secret={1}"
				"&categoryId={2}"
				"&ll={3}"
				"&v={4}"
				"&radius={5}"
				"&limit=100"
			).format(CLIENT_ID, CLIENT_SECRET, CATEGORY_ID, coords, api_version, radius)
			# make the request
			response = urllib.request.urlopen(url)
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
		''' Retrives business data using the Foursquare API for Boston and SF'''
		pp = foursquare_retrieve.pp
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
		response = foursquare_retrieve.get_data_by_city("SF")
		print("Got the following number of businesses in SF:")
		print(len(response))
		repo.dropCollection( sf_coll )
		repo.createCollection( sf_coll )
		repo[ sf_coll ].insert_many(response)
		repo[ sf_coll ].metadata( {'complete':True} )
		print(repo[ sf_coll ].metadata())
		
		response = foursquare_retrieve.get_data_by_city("Boston")
		print("Got the following number of businesses in Boston:")
		print(len(response))
		repo.dropCollection( boston_coll )
		repo.createCollection( boston_coll )
		repo[ boston_coll ].insert_many(response)
		repo[ boston_coll ].metadata( {'complete':True} )
		print(repo[ boston_coll ].metadata())
		

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
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')		

foursquare_retrieve.execute()