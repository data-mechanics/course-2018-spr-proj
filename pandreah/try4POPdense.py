import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from math import *

'''In this script I am calculating weather each home has a Hubway Station within a 1KM radius.
I am also calculating the density of homes within a 3KM ring starting 1KM away for each Hubway Station.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics  '''
class try4POPdense(dml.Algorithm):
    contributor = 'pandreah'
    reads = ['pandreah.propertyA', 'pandreah.hubwayStations']
    writes = ['pandreah.popDense']

###########################################################################
##      This was taken from the class website.                           ##
###########################################################################

    def select(R, s):
        return [t for t in R if s(t)]



    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')

        repo.dropCollection("popDense")
        repo.createCollection("popDense")

        
## This is where I'm setting up my sources in order to use the data to get the densities
        h = repo['pandreah.propertyA']
        homes = list(h.find())

        s = repo['pandreah.hubwayStations']
        stations = list(s.find())

 ## These are the radii I will use for getting the homes count and the Hubways count.
        radius = 1  

        rad_th = 4

        counting_homes = 0

        for home in homes:
            if trial == True:
                if counting_homes >= 25:
                    break
##            
            counting_homes += 1

            
            center_lat =round(float(home["Y" ]),8)
            center_lng =round(float(home["﻿X" ]),8)


            count_hubs = 0

            for station in stations:
                count_hubs += 1

                print("this is the stations that we're looking at: ", station, "this is it's lat/lgn: ", round(float(station["Latitude" ]),8))

                o_lat = round(float(station["Latitude" ]),8)
                o_long = round(float(station["Longitude"]),8)
            

                hubways_lt4KM_mt1KM = 0

                distanceK = 6371 * acos(cos(radians(90 - center_lat)) * cos(radians(90 - o_lat)) + sin(radians(90 - center_lat)) * sin(radians(90 - o_lat)) * cos(radians(center_lng - o_long)))
                
                if distanceK <= rad_th:
                    if distanceK >= radius:
                        hubways_lt4KM_mt1KM = 1
                            
                                           

            r = {'lat': center_lat, 'lng': center_lng, 'homes_around': 0, 'hubways_lt4KM_mt1KM': hubways_lt4KM_mt1KM}

            repo.pandreah.popDense.insert_one(r)

        new_homes = repo['pandreah.popDense']

#        print(list(new_homes.find()))

        s = lambda x: (x["hubways_lt4KM_mt1KM"] == 1)
        H = try4POPdense.select(new_homes.find(), s)

        print(H)

        new_house_index = 0
        for new_house in range(len(H)):

            if trial == True:
                if new_house_index >= 10:
                    break

            print("This is the house that we're looking at: ", new_house_index)

            
            idh = H[new_house_index]["_id"]
            center_lat_new =round(float(H[new_house_index]["lat"]),8)
            center_lng_new =round(float(H[new_house_index]["lng"]),8)

            
            counting_houses_in_ring = 0
#            print("this should always always be 0: ", counting_houses_in_ring)
            other_new_house_index = 0
            for other_new_house in range(len(H)):
                if H[other_new_house_index]["_id"] != idh:
                    o_lat_new = round(float(H[other_new_house_index]["lat"]),8)
                    o_long_new = round(float(H[other_new_house_index]["lng"]),8)
                
                    distanceK_new = 6371 * acos(round(cos(radians(90 - center_lat_new)) * cos(radians(90 - o_lat_new)) + sin(radians(90 - center_lat_new)) * sin(radians(90 - o_lat_new)) * cos(radians(center_lng_new - o_long_new)), 8))

                    if distanceK_new <= radius:
                        counting_houses_in_ring += 1
                other_new_house_index += 1
            H[new_house]["homes_around"] = counting_houses_in_ring
            new_house_index += 1


        repo.dropCollection('pandreah.popDense')
        repo.createCollection('pandreah.popDense')
        repo['pandreah.popDense'].insert_many(H)
                        

            
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

        this_script = doc.agent('alg:pandreah#popDense', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        try4POPdense = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(try4POPdense, this_script)
        doc.usage(try4POPdense, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Property+Density+Per+House&$select=type,OBJECTID,id, lat, long, homes_around, hubways_lt4KM_mt1KM'
                  }
                  )

        popDense = doc.entity('dat:pandreah#popDense', {prov.model.PROV_LABEL:'Hubway And Home Desnity for Homes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(popDense, this_script)
        doc.wasGeneratedBy(popDense, try4POPdense, endTime)
        doc.wasDerivedFrom(popDense, resource, try4POPdense, try4POPdense, try4POPdense)

        repo.logout()
                  
        return doc
    
if __name__ == "__main__":
    try4POPdense.execute()
    doc = try4POPdense.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))

