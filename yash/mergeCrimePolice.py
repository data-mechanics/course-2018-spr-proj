import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint

class mergeCrimePolice(dml.Algorithm):
    contributor = 'ybavishi'
    reads = ['yash.rentData','yash.policeStationData', 'yash.crimesData']
    writes = ['yash.crimePoliceData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yash', 'yash')
        areas = repo['yash.rentData']
        crimes = repo['yash.crimesData']
        police = repo['yash.policeStationData']
        # pprint.pprint(prices.find_one({"RegionName":"02134"}))
        area_records = []
        for area in areas.find():
            area_dict = {}
            area_dict['Region'] = area['city']
            police_station = police.find_one({"NEIGHBORHOOD":area['city']})
            if police_station:
                polices_station_district = police_station['DISTRICT']
                count = 0
                for i in crimes.find({'DISTRICT': polices_station_district}):
                    count += 1
                area_dict['Police_Station'] = 'yes'
                area_dict['Crimes_count'] = count
            else:
                area_dict['Police_Station'] = 'no'
                area_dict['Crimes_count'] = 'N/A'
            area_records.append(area_dict.copy())


        repo.dropCollection("yash.crimePoliceData")
        repo.createCollection("yash.crimePoliceData")
        repo['yash.crimePoliceData'].insert_many(area_records)
        repo['yash.crimePoliceData'].metadata({'complete':True})
        print(repo['yash.crimePoliceData'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

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
        repo.authenticate('yash', 'yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ybavishi#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ybavishi#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:mergeCrimePolice', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:rentData', {'prov:label':'Rent Areas', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:policeStationData', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource'})
        resource3 = doc.entity('dat:crimesData', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_prices, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_prices, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        prices = doc.entity('dat:crimePoliceData', {prov.model.PROV_LABEL:'Crimes in Rent Areas', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)
        doc.wasDerivedFrom(prices, resource2, get_prices, get_prices, get_prices)
        doc.wasDerivedFrom(prices, resource3, get_prices, get_prices, get_prices)


      
        repo.logout()
                  
        return doc

mergeCrimePolice.execute()
doc = mergeCrimePolice.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

#eof