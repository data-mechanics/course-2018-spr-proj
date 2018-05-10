# Filename: StatLibrary.py
# Author: Brooke E Mullen <bemullen@bu.edu>
# Description: Statistic utilities.

import urllib.request
import subprocess
from urllib.request import quote 
import numpy as np
import math
import json
import csv
import dml
import prov.model
import datetime
import uuid
import xmltodict
import pandas
from random import shuffle
from math import sqrt

class StatLibrary(dml.Algorithm):
	contributor = "bemullen_crussack_dharmesh_vinwah"
	reads =['bemullen_crussack_dharmesh_vinwah.libraries']
	writes =['bemullen_crussack_dharmesh_vinwah.libraryNoStu',
	'bemullen_crussack_dharmesh_vinwah.libraryStu',
	'bemullen_crussack_dharmesh_vinwah.correlation']

	@staticmethod
	def execute(trial=False):
		startTime = datetime.datetime.now()

		######### set up the db connection ##########

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('bemullen_crussack_dharmesh_vinwah','bemullen_crussack_dharmesh_vinwah')

		
		###### RUN STAT ANALYSIS ########
		r = repo['bemullen_crussack_dharmesh_vinwah.libraries'].find()
		r = list(r)

		students = [x for x in r if x["ETL_LOAD_DATE"][5:7] in ["09","10","11","02","03","04"]]
		noStudents = [x for x in r if x["ETL_LOAD_DATE"][5:7] in ["06","07","08"]]

		dec = [x for x in r if x["ETL_LOAD_DATE"][5:7] in ["12"]] #and x["ETL_LOAD_DATE"][8:10] in []]
		jan = [x for x in r if x["ETL_LOAD_DATE"][5:7] in ["01"]]

		decStu = [x for x in dec if x["ETL_LOAD_DATE"][8:10] in 
		["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15"]]

		decNoStu = [x for x in dec if x["ETL_LOAD_DATE"][8:10] not in
		["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15"]]

		janStu = [x for x in jan if x["ETL_LOAD_DATE"][8:10] not in
		["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15"]]
		janNoStu = [x for x in jan if x["ETL_LOAD_DATE"][8:10] in
		["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15"]]

		students += decStu
		students += janStu
		noStudents += decNoStu
		noStudents += janNoStu

		non_x = []
		non_y = []
		stu_x = []
		stu_y = []


		for i in range(len(students)-2):
			if len(students[i]['CTY_SCR_NBR_DY_01']) > 0:
				stu_x.append(float(students[i]['CTY_SCR_NBR_DY_01'])) # attendees
			if len(students[i]['CTY_SCR_DAY']) > 0:   
				stu_y.append(float(students[i]['CTY_SCR_DAY']))
		
		for i in range(len(noStudents)-2):
			if (len(noStudents[i]['CTY_SCR_NBR_DY_01']) > 0):
				non_x.append(float(noStudents[i]['CTY_SCR_NBR_DY_01']))
			if (len(noStudents[i]['CTY_SCR_DAY']) > 0):
				non_y.append(float(noStudents[i]['CTY_SCR_DAY']))
		

		def avg(x): # Average
			return sum(x)/len(x)

		def stddev(x): # Standard deviation.
			m = avg(x)
			return math.sqrt(sum([(xi-m)**2 for xi in x])/len(x))

		def cov(x, y): # Covariance.
			return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

		def corr(x, y):                 # Correlation coefficient.
			if stddev(x)*stddev(y) != 0:
				return cov(x, y)/(stddev(x)*stddev(y))

		noncor = corr(non_x,non_y)
		stucor = corr(stu_x,stu_y)

		repo.dropCollection("libraryNoStu")
		repo.createCollection("libraryNoStu")
		repo['bemullen_crussack_dharmesh_vinwah.libraryNoStu'].insert_many(noStudents)
		repo['bemullen_crussack_dharmesh_vinwah.libraryNoStu'].metadata({'complete':True})
		print(repo['bemullen_crussack_dharmesh_vinwah.libraryNoStu'].metadata())

		repo.dropCollection("libraryStu")
		repo.createCollection("libraryStu")
		repo['bemullen_crussack_dharmesh_vinwah.libraryStu'].insert_many(students)
		repo['bemullen_crussack_dharmesh_vinwah.libraryStu'].metadata({'complete':True})
		print(repo['bemullen_crussack_dharmesh_vinwah.libraryStu'].metadata())

		jsondata = '{"correlation coefficient for students in session":'+str(stucor)+\
		', "correlation coefficient for students not in session":'+str(noncor)+'}'
		
		jsonToPython = json.loads(jsondata)

		cor = []
		cor.append(jsonToPython)

		repo.dropCollection('correlation')
		repo.createCollection('correlation')
		repo['bemullen_crussack_dharmesh_vinwah.correlation'].insert_many(cor)
		repo['bemullen_crussack_dharmesh_vinwah.correlation'].metadata({'complete':True})
		print(repo['bemullen_crussack_dharmesh_vinwah.correlation'].metadata())

		endTime = datetime.datetime.now()

		return {'start':startTime,'end':endTime}


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(),startTime= None,endTime=None):
		''' 
    Create the provenance document describing everything happening in this 
    script. Each run of the script will generate a new document describing that
    innocation event.
    '''

    # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo

		repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
		doc.add_namespace('log', 'http://datamechanics.io/log/')
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
		doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
		doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')
		doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')

    ###### this_script ##########
        
		this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#StatLibrary',\
			{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ###### rescource_libraries ##########
        
		resource_libraries = doc.entity('bdp:5bce8e71-5192-48c0-ab13-8faff8fef4d7',
			{'prov:label':'Libraries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
    ####### get_libraries ##########
        
		get_statlib = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
			{'prov:label': ('Calculate the correlation coefficents of Boston library users'
				'during university session and not in session compared to Boston\'s city score')})
        
		doc.wasAssociatedWith(get_statlib, this_script)

		doc.usage(get_statlib, resource_libraries, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval'
				})

		libraries = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#statlib',
			{prov.model.PROV_LABEL:'Libraries Metrics with correlation coefficents',
			prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(libraries, this_script)
		doc.wasGeneratedBy(libraries, get_statlib, endTime)
		doc.wasDerivedFrom(libraries, resource_libraries, get_statlib,get_statlib, get_statlib)

		repo.logout()
		return doc



