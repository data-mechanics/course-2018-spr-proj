import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from math import *

'''In this script I am calculating the density of homes in a 1KM radius around each Hubway station.
I am also calculating the density of Hubways in a 3KM radius around each Hubway station.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics '''

class dense4Hub(dml.Algorithm):
    contributor = 'pandreah'
    reads = ['pandreah.propertyA', 'pandreah.hubwayStations']
    writes = ['pandreah.popHubway']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')

        repo.dropCollection("popHubway")
        repo.createCollection("popHubway")

## This is where I'm setting up my sources in order to use the data to get the densities
        h = repo['pandreah.propertyA']
        homes = list(h.find())

        s = repo['pandreah.hubwayStations']
        stations = list(s.find())
        
#        print(homes)
#        print('Is this thing even working??')

## These are the radii I will use for getting the homes count and the Hubways count.
        radius = 1  # radius = 1KM
        rad_th = 3  # radius = 3KM

        counting_hubs = 0    #counter for trial mode
        
        for station in stations:

            if trial == True:
                if counting_hubs == 100:
                    break
            
            counting_hubs += 1
            
#            print("This is is how many Hubways I've looked at: ", counting_hubs - 1)

#Getting latitude and longitude for the Hubway station that we're looking at currently
            center_lat = float(station["Latitude"])
            center_lng = float(station["Longitude"])


## We will start a for loop that gets the homes inside the 1KM radius around the Hubway Station we're currently looking at        
            count_Homes = 0
            for house in homes:

                if trial == True:
                    if count_Homes == 50:
                        break

                #Distance calculated according to the Haversine formula
                distanceK = 6371 * acos(cos(radians(90 - center_lat)) * cos(radians(90 - float(house["Y" ]))) + sin(radians(90 - center_lat)) * sin(radians(90 - float(house["Y" ]))) * cos(radians(center_lng - float(house["﻿X" ]))))

                if distanceK <= radius:   #comparing the distance of each home with the 1KM radius
                    count_Homes += 1
#            print(count_Homes)
            
## We will start a for loop that gets the other Hubway stations inside the 3KM radius around the Hubway Station we're currently looking at
            count_Other_Hubways = 0
            count_Other_Hubways_trial = 0
#            print("This ", center_lat, center_lng, " is the Latitude/Longitude of central Hubway #", counting_hubs)
            for hubs in stations:

                if trial == True:
                    if count_Other_Hubways_trial == 100:
                        break
                
                if station["_id" ] != hubs["_id" ]:

                    #Distance calculated according to the Haversine formula
                    distanceerK= 6371 * acos(cos(radians(90 - center_lat)) * cos(radians(90 - float(hubs["Latitude" ]))) + sin(radians(90 - center_lat)) * sin(radians(90 - float(hubs["Latitude" ]))) * cos(radians(center_lng - float(hubs["Longitude" ]))))

                    if distanceerK <= rad_th:
                        count_Other_Hubways += 1
                        
                count_Other_Hubways_trial += 1
                
#            print(count_Homes)

            r = {'lat': center_lat, 'lng': center_lng, 'houses_1KM': count_Homes , 'hubways_3KM': count_Other_Hubways}

            repo.pandreah.popHubway.insert_one(r)

        
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
        repo.authenticate('pandreah', 'pandreah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pandreah#popHubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        dense4Hub = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(dense4Hub, this_script)
        doc.usage(dense4Hub, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Property+Density+Per+Hubway&$select=type,OBJECTID,id,info, lat, long, houses_1KM, hubways_3KM'
                  }
                  )

        popHubway = doc.entity('dat:pandreah#popHubway', {prov.model.PROV_LABEL:'Hubway And Home Desnity', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(popHubway, this_script)
        doc.wasGeneratedBy(popHubway, dense4Hub, endTime)
        doc.wasDerivedFrom(popHubway, resource, dense4Hub, dense4Hub, dense4Hub)

        repo.logout()
                  
        return doc

if __name__ == "__main__":
    dense4Hub.execute()
    doc = dense4Hub.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))

