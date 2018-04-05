import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from ast import literal_eval as make_tuple

class example(dml.Algorithm):
	contributor = 'csuksan_tedkong'
	reads = []
	writes = ['csuksan_tedkong.hub', 'csuksan_tedkong.mbta_station', 'csuksan_tedkong.streetlight', 'csuksan_tedkong.crime', 'csuksan_tedkong.meter']

	@staticmethod
	def execute(trial = False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('csuksan_tedkong', 'csuksan_tedkong')


		#Dataset 1: Hubway Stations
		url='http://hubwaydatachallenge.org/api/v1/station/?format=json&username=csuksangium&api_key=a94a345a88c6b2fb455227cfacb5612b10b2f0bc'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		s = json.dumps(r, sort_keys=True, indent=2)
		rr = json.loads(s)
		repo.dropCollection("hub")
		repo.createCollection("hub")
		repo['csuksan_tedkong.hub'].insert_one(rr)
		repo['csuksan_tedkong.hub'].metadata({'complete':True})
		print(repo['csuksan_tedkong.hub'].metadata())


		#Dataset 2: MBTA Stops
		#transformation 1: combine mbta stops w/ hubway stations to know which areas currently lack transportation
		hubway_url = 'http://hubwaydatachallenge.org/api/v1/station/?format=json&username=csuksangium&api_key=a94a345a88c6b2fb455227cfacb5612b10b2f0bc'
		mbta_url = 'https://api-v3.mbta.com/stops'
		response1 = urllib.request.urlopen(mbta_url).read().decode("utf-8")
		response2 = urllib.request.urlopen(hubway_url).read().decode("utf-8")

		mbta_json = json.loads(response1)
		hubway_json = json.loads(response2)

		#ID, type, latitudes, longitudes
		hubway_mbta = {'Entries':[]}
		hubway_mbta['Entries'] += [{'Id':0000,'Type':'mbta','coordinates':[10,5]}]
		print(hubway_mbta)
		for i in range(len(hubway_json['objects'])):
		    hubway_mbta['Entries'] += [{'ID':i,'Type':'Hubway','Latitudes':hubway_json['objects'][i]['point']['coordinates'][1],'Longitudes':hubway_json['objects'][i]['point']['coordinates'][0]}]

		for i in range(len(list(mbta_json['data']))):
		    hubway_mbta['Entries'] += [{'ID':i + len(hubway_json['objects']),'Type':'MBTA Stops','Latitudes':mbta_json['data'][i]['attributes']['latitude'],'Longitudes':mbta_json['data'][i]['attributes']['longitude']}]
		repo.dropCollection("mbta_station")
		repo.createCollection("mbta_station")
		repo['csuksan_tedkong.mbta_station'].insert_one(hubway_mbta)
		repo['csuksan_tedkong.mbta_station'].metadata({'complete':True})
		print(repo['csuksan_tedkong.mbta_station'].metadata())

		    

		#Dataset #3: Boston Crime Report
		#Transformation #2: crime report from .csv to pandas dataframe for further manipulation (filtering, selecting, etc.) and eventually json
		url4 = "https://data.boston.gov/dataset/eefad66a-e805-4b35-b170-d26e2028c373/resource/ba5ed0e2-e901-438c-b2e0-4acfc3c452b9/download/crime-incident-reports-july-2012---august-2015-source-legacy-system.csv"
		data_crime = pd.read_csv(url4, encoding = "utf-8")
		#transform crime data to fit with parking meter locations
		data_interim =  data_crime[['Location']].copy()
		#add custom identifier 'crime' to data frame column
		crime_id = ['crime'] * 268056
		data_interim['ID'] = crime_id
		#remove (0.0, 0.0) from data
		data_interim = data_interim[data_interim['Location'] != '(0.0, 0.0)']
		crime_temp = data_interim['Location'].tolist()
		crime_longitude = [make_tuple(x) for x in crime_temp]
		#separate longtitude and latitude from tuple form
		crime_long = [x[1] for x in crime_longitude]
		crime_lat = [x[0] for x in crime_longitude]
		data_interim['Latitude'] = crime_lat
		data_interim['Longitude'] = crime_long
		#new data frame with ID, latitude, and longtitude
		data_interim = data_interim.drop(['Location'], axis=1)
		r = json.loads(data_interim.to_json( orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("crime")
		repo.createCollection("crime")
		repo['csuksan_tedkong.crime'].insert_many(r)
		repo['csuksan_tedkong.crime'].metadata({'complete':True})



		#Dataset 4: Streetlight locations
		#Continuation of Transformation 2: 
		#Convert streetlight .csv to pandas dataframe for manipulation and merging with crimes to help find relationship between the two later in the project
		url3 = "https://data.boston.gov/dataset/52b0fdad-4037-460c-9c92-290f5774ab2b/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5/download/streetlight-locations.csv"
		data = pd.read_csv(url3, encoding = "utf-8")
		#select relevant columns
		data_light = data[['TYPE','Lat','Long']].copy()
		#Convert to appropriate names
		data_light.columns = ['ID', 'Latitude', 'Longitude']
		data_interim2 = data_interim.copy()
		frames2 = [data_interim2,data_light]
		result2 = pd.concat(frames2)
		result2 = result2.reset_index(drop=True)
		r = json.loads(result2.to_json( orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("streetlight")
		repo.createCollection("streetlight")
		repo['csuksan_tedkong.streetlight'].insert_many(r)
		repo['csuksan_tedkong.streetlight'].metadata({'complete':True})
		print(repo['csuksan_tedkong.streetlight'].metadata())



		#Dataset #5: Parking meter locations
		#Transformation #3: parsing parking meter to format friendly with merging with crimes to help determine place
		#inappropriate for placing hubway station
		url5 = "http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.csv"
		data = pd.read_csv(url5, encoding = "utf-8")
		#convert parking meter to become same format as crime data frame for merging
		data_meter = data[['X', 'Y']].copy()
		data_meter.columns = ['Longitude', 'Latitude']
		#create ID 'parking_meter' for meter
		meter_id = ['parking_meter'] * 6955
		data_meter['ID'] = meter_id
		#reorder and name to match with crime data frames
		column_order = ['ID','Latitude','Longitude']
		data_meter=data_meter.reindex(columns=column_order)
		frames = [data_interim, data_meter]
		#join frames
		result = pd.concat(frames)
		#reset index
		result = result.reset_index(drop=True)
		r = json.loads(result.to_json( orient='records'))
		s = json.dumps(r, sort_keys=True, indent=2)
		repo.dropCollection("meter")
		repo.createCollection("meter")
		repo['csuksan_tedkong.meter'].insert_many(r)
		repo['csuksan_tedkong.meter'].metadata({'complete':True})


		repo.logout()

		return

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('csuksan_tedkong', 'csuksan_tedkong')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:csuksan_tedkong#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_hub = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_mbta = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_streetlight = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_meter = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_hub, this_script)
		doc.wasAssociatedWith(get_mbta, this_script)
		doc.wasAssociatedWith(get_streetlight, this_script)
		doc.wasAssociatedWith(get_crime, this_script)
		doc.wasAssociatedWith(get_meter, this_script)

		doc.usage(get_hub, resource, startTime, None,
		{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':'?type=Hubway+Station&$select=meta,objects'
			}
			)
		doc.usage(get_mbta, resource, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':'?type=MBTA+Stop&$select=links,included,data,links,id,attributes'
			}
			)
		doc.usage(get_streetlight, resource, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':'?type=Streetlight+Location&$select=the_geom,OBJECTID,TYPE,Lat,Long'
			}
			)
		doc.usage(get_crime, resource, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':'?type=Crime+Rate&$select=COMPNOS,NatureCode,INCIDENT_TYPE_DESCRIPTION,MAIN_CRIMECODE,REPTDISTRICT,REPORTINGAREA,FROMDATE,WEAPONTYPE'
			}
			)
		doc.usage(get_meter, resource, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval',
			'ont:Query':'?type=Parking+Meter&$select=X,Y,OBJECTID_1,OBJECTID,TYPE,ELEV'
			}
			)

		mbta = doc.entity('dat:csuksan_tedkong#mbta_station', {prov.model.PROV_LABEL:'MBTA Stations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(mbta, this_script)
		doc.wasGeneratedBy(mbta, get_mbta, endTime)
		doc.wasDerivedFrom(mbta, resource, get_mbta, get_mbta, get_mbta)

		hub = doc.entity('dat:csuksan_tedkong#hub', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hub, this_script)
		doc.wasGeneratedBy(hub, get_hub, endTime)
		doc.wasDerivedFrom(hub, resource, get_hub, get_hub, get_hub)

		streetlight = doc.entity('dat:csuksan_tedkong#streetlight', {prov.model.PROV_LABEL:'Streetlight locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(streetlight, this_script)
		doc.wasGeneratedBy(streetlight, get_streetlight, endTime)
		doc.wasDerivedFrom(streetlight, resource, get_streetlight, get_streetlight, get_streetlight)

		crime = doc.entity('dat:csuksan_tedkong#crime', {prov.model.PROV_LABEL:'Crime rates and location', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crime, this_script)
		doc.wasGeneratedBy(crime, get_crime, endTime)
		doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

		meter = doc.entity('dat:csuksan_tedkong#meter', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(meter, this_script)
		doc.wasGeneratedBy(meter, get_meter, endTime)
		doc.wasDerivedFrom(meter, resource, get_meter, get_meter, get_meter)

		repo.logout()
		return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

