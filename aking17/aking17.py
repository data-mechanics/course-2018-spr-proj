import csv
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import pymongo
import pandas as pd

class hubway_trips(dml.Algorithm):
	contributor = 'aking17'
	reads = []
	writes = ['aking17.hubway_trips']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		#read csv file
		data = pd.read_csv('http://datamechanics.io/data/aking17/hubway_trips.csv')
		r = json.loads(data.to_json(orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("hubway_trips")
		repo.createCollection("hubway_trips")
		repo['aking17.hubway_trips'].insert_many(r)
		repo['aking17.hubway_trips'].metadata({'complete':True})
		print(repo['aking17.hubway_trips'].metadata())
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aking17/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/aking17/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://hubwaydatachallenge.org')

		this_script = doc.agent('alg:aking17#hubway_trips', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		get_hubway_trips = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_hubway_trips, this_script)
		doc.usage(get_hubway_trips, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'seq_id,hubway_id,status,duration,start_date,strt_statn,end_date,end_statn,bike_nr,subsc_type,zip_code,birth_date,gender'
					}
					)

		hubway_trips = doc.entity('dat:aking17#hubway_trips', {prov.model.PROV_LABEL:'Hubway Trips', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hubway_trips, this_script)
		doc.wasGeneratedBy(hubway_trips, get_hubway_trips, endTime)
		doc.wasDerivedFrom(hubway_trips, resource, get_hubway_trips, get_hubway_trips, get_hubway_trips)

		repo.logout()
		          
		return doc

class hubway_stations(dml.Algorithm):
	contributor = 'aking17'
	reads = []
	writes = ['aking17.hubway_stations']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		#read csv file
		data = pd.read_csv('http://datamechanics.io/data/aking17/hubway_stations.csv')
		r = json.loads(data.to_json(orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("hubway_stations")
		repo.createCollection("hubway_stations")
		repo['aking17.hubway_stations'].insert_many(r)
		repo['aking17.hubway_stations'].metadata({'complete':True})
		print(repo['aking17.hubway_stations'].metadata())
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aking17/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/aking17/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://hubwaydatachallenge.org')

		this_script = doc.agent('alg:aking17#hubway_stations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		get_hubway_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_hubway_stations, this_script)
		doc.usage(get_hubway_stations, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'id,terminal,station,municipal,lat,lng,status'
					}
					)

		hubway_stations = doc.entity('dat:aking17#hubway_stations', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hubway_stations, this_script)
		doc.wasGeneratedBy(hubway_stations, get_hubway_stations, endTime)
		doc.wasDerivedFrom(hubway_stations, resource, get_hubway_stations, get_hubway_stations, get_hubway_stations)

		repo.logout()
		          
		return doc

class bike_availability(dml.Algorithm):
	contributor = 'aking17'
	reads = []
	writes = ['aking17.bike_availability']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		#read csv file
		data = pd.read_csv('http://datamechanics.io/data/aking17/bike_availability.csv')
		r = json.loads(data.to_json(orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("bike_availability")
		repo.createCollection("bike_availability")
		repo['aking17.bike_availability'].insert_many(r)
		repo['aking17.bike_availability'].metadata({'complete':True})
		print(repo['aking17.bike_availability'].metadata())
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

			# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aking17/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/aking17/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://hubwaydatachallenge.org')

		this_script = doc.agent('alg:aking17#bike_availability', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		get_bike_availability = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_bike_availability, this_script)
		doc.usage(get_bike_availability, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'Terminal Number,Station Name,Status,Start,End,Duration'
					}
					)

		bike_availability = doc.entity('dat:aking17#bike_availability', {prov.model.PROV_LABEL:'Bike Availability', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(bike_availability, this_script)
		doc.wasGeneratedBy(bike_availability, get_bike_availability, endTime)
		doc.wasDerivedFrom(bike_availability, resource, get_bike_availability, get_bike_availability, get_bike_availability)

		repo.logout()
		          
		return doc

class traffic(dml.Algorithm):
	contributor = 'aking17'
	reads = []
	writes = ['aking17.traffic']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		#reader = csv.reader(open('hubway_trips.csv'))
		#d = {}

		url = 'http://datamechanics.io/data/trafficjam.json'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("traffic")
		repo.createCollection("traffic")
		repo['aking17.traffic'].insert_many(r)
		repo['aking17.traffic'].metadata({'complete':True})
		print(repo['aking17.traffic'].metadata())
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aking17/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/aking17/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://datamechanics.io')

		this_script = doc.agent('alg:aking17#traffic', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_traffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_traffic, this_script)
		doc.usage(get_traffic, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'inject_date, street,city, delay,endNode,Length,level,roadType,speed,startTime,endTime'
					}
					)

		traffic = doc.entity('dat:aking17#traffic', {prov.model.PROV_LABEL:'Bike Availability', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(traffic, this_script)
		doc.wasGeneratedBy(traffic, get_traffic, endTime)
		doc.wasDerivedFrom(traffic, resource, get_traffic, get_traffic, get_traffic)

		repo.logout()
		          
		return doc

class redline(dml.Algorithm):
	contributor = 'aking17'
	reads = []
	writes = ['aking17.redline']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')

		url = 'http://datamechanics.io/data/redlineStreets.json'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("redline")
		repo.createCollection("redline")
		repo['aking17.redline'].insert_many(r)
		repo['aking17.redline'].metadata({'complete':True})
		print(repo['aking17.redline'].metadata())
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('aking17', 'aking17')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aking17/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/aking17/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://datamechanics.io')

		this_script = doc.agent('alg:aking17#redline', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_redline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_redline, this_script)
		doc.usage(get_redline, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'_id,lat,lon,streets'
					}
					)

		redline = doc.entity('dat:aking17#redline', {prov.model.PROV_LABEL:'Redline', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(redline, this_script)
		doc.wasGeneratedBy(redline, get_redline, endTime)
		doc.wasDerivedFrom(redline, resource, get_redline, get_redline, get_redline)

		repo.logout()
		          
		return doc


hubway_trips.execute()
b = hubway_trips.provenance()
print(b.get_provn())
print(json.dumps(json.loads(b.serialize()), indent=4))

bike_availability.execute()
d = traffic.provenance()
print(d.get_provn())
print(json.dumps(json.loads(d.serialize()), indent=4))

traffic.execute()
a = traffic.provenance()
print(a.get_provn())
print(json.dumps(json.loads(a.serialize()), indent=4))



hubway_stations.execute()
c = traffic.provenance()
print(c.get_provn())
print(json.dumps(json.loads(c.serialize()), indent=4))



redline.execute()
e = traffic.provenance()
print(e.get_provn())
print(json.dumps(json.loads(e.serialize()), indent=4))


## eof