import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import pymongo
import uuid
import re

class foursquare_analysis(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.sf_businesses', 'agoncharova_lmckone.boston_businesses']
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
		return result

	def filter_4sq_data_by_office_type(data):
		'''
		Returns data that contains only "Coworking Space", or "Tech Startup", or
		"Conference Room"
		# for boston area, returns 167 pts
		'''
		fa = foursquare_analysis
		result = []
		filter_by = ["Coworking Space", "Tech Startup", "Conference Room", "IT Services"] 
		for item in data:
			cats = item["categories"]
			for cat in cats:
				if cat["shortName"] in filter_by:
					result.append(item)
		return result

	@staticmethod
	def filter_permits(data):
		'''
		Filters permit data for Boston and permit type = 1
		'''
		fa = foursquare_analysis
		
		return 0

	@staticmethod
	def execute():
		fa = foursquare_analysis
		data_4sq = fa.get_4sq_data("Boston")
		data_4sq = fa.filter_4sq_data_by_office_type(data_4sq)
		return 0


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return 0

foursquare_analysis.execute()

