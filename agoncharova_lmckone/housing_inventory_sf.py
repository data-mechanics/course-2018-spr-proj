import urllib.request
import json
import csv
import pprint
import dml
import prov.model
import datetime
import uuid
import os
import pandas as pd


class housing_inventory_sf(dml.Algorithm):
	pp = pprint.PrettyPrinter(indent=2)
	
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = []

	# urls for the json data
	_2011 = 'https://data.sfgov.org/resource/pwiv-ej3p.json'
	_2012 = 'https://data.sfgov.org/resource/a64c-96a5.json'
	_2013 = 'https://data.sfgov.org/resource/sjse-8gyy.json'
	_2014 = 'https://data.sfgov.org/resource/b8d6-zthg.json'	
	# the ugly duckling 
	_2015 = 'https://data.sfgov.org/api/views/4htx-8nvv/files/ef13cd5c-a70b-4699-961a-ccc9c356e926?filename=2015_datasf.xlsx'
	_2016 = 'https://data.sfgov.org/resource/4jbp-vami.json'
	all_urls = {_2011, _2012, _2013, _2015, _2014, _2016}


	def convert_xlsx_to_csv(fn, sheet_name):
		'''
		Use this to convert the 2015 data into csv
		Need pandas and xlrd packages for this

		'''
		# filenames		
		fn_exl = fn + ".xlsx"		
		fn_csv = fn + ".csv"
		fn_json = fn + ".json"

		# first convert from xml to csv via pandas
		data_xls = pd.read_excel(fn_exl, sheet_name, index_col=None)
		data_xls.to_csv(fn_csv, encoding='utf-8', index=False)

		# then convert from csv to json
		# source: https://stackoverflow.com/questions/19697846/python-csv-to-json
		csvfile = open(fn_csv, 'r')
		# TODO: maybe isolate this somehow (extract from csv or something)
		fieldnames = ("APPL_NO", "APP_ID", "FORM", "ST_NUM", "ST_NAME", "ST_TYPE",
			"Address", "BLOCK", "LOT", "BLKLOT", "UNITS", "NETUNITS", "AFFHSG", "AFFTARGET", "DESCRIPT", 
			"EXISTUSE", "PROPUSE", "ACTION", "ACTDATE", "STAFF", "GEN_ZONE", "ZONING",
			"PDx", "SUPEx", "merger", "DEVELOPERN", "NOTES", "DEVNAME", "AGENCY", "TYPE",
			"OCII_AREA", "NEW_OR_REH", "Tenure", "AMI_Max", "PLANAREA", "DISTRICT",
			"PD_NO", "SUPDIST", "SUPERVISOR", "X", "Y", "EastSoma", "Mission", "CentralWat", 
			"MarketOcta", "WestSoma", "ShowplaceP")			
		reader = csv.DictReader( csvfile, fieldnames)
		json_data = []
		x = 0 
		for row in reader:
			print(row)
			str_json = json.dumps(row)
			var = json.loads(str_json)
			json_data.append(var)
		
		# delete intermediate files and return an array of json objects
		os.remove(fn_exl)
		os.remove(fn_csv)
		return json_data		


	def fetch_by_url(url):
		type = url[-4:]
		data = []
		if(type == "json"):
			print("json file")
			response = urllib.request.urlopen(url)
			data = json.load(response)
		if(type == "xlsx"):
			print("xlsx file")
			fn = "2015_housing_inventory_sf"
			sheet_name = "2015Completes"
			# fetch and dump into an xlsx file
			fn_exl = fn + ".xlsx"
			response = urllib.request.urlretrieve(url, fn_exl)
			data = housing_inventory_sf.convert_xlsx_to_csv(fn, sheet_name)		
		return data



	def save_to_db(data, coll_name):
		db_name = 'agoncharova_lmckone'
		coll_name = db_name + "." + coll_name
		print("collection name is: " + coll_name)
		# setup
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		print("About to save data to the following collection: " + coll_name)
		repo.dropCollection( coll_name )
		repo.createCollection( coll_name )
		repo[ coll_name ].insert_many(data)
		repo[ coll_name ].metadata( {'complete':True} )
		print("Saved data to the following collection: " + coll_name)
		print(repo[ coll_name ].metadata())

	@staticmethod
	def execute(trial = False):
		'''
		Retrieves the Housing Inventory datasets from 
		thta portal for years [2011-2016]
		'''
		startTime = datetime.datetime.now()

		hi = housing_inventory_sf
		for url in hi.all_urls:
			data = hi.fetch_by_url(url)
			if(len(data) == 0):
				raise ValueError('No data was fetched for url: ' + url)
			type = url[-4:]
		# probably dump the 2015 data into a diff collection
			if(type == "xlsx"):
				hi.save_to_db(data, "2015_housing_inventory_sf")
			else:
				hi.save_to_db(data, "not_2015_housing_inventory_sf")					
		return 0

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return 0		

housing_inventory_sf.execute()