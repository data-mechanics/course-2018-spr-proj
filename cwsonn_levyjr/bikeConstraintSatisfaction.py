import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math
import statistics
import matplotlib.pyplot as plt

class bikeConstraintSatisfaction(dml.Algorithm):
    contributor = 'cwsonn_levyjr'
    reads = ['cwsonn_levyjr.Cbikepath', 'cwsonn_levyjr.bikerack']
    writes = ['cwsonn_levyjr.bikeComparisonCam']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cwsonn_levyjr', 'cwsonn_levyjr')
        
        Cbikepath = repo['cwsonn_levyjr.Cbikepath'].find()
    
        #Get Cambridge bike path coordinates
        bikePathCoords = []
        lenlst = []
        for c in Cbikepath:
            pathCoords = c["geometry"]["coordinates"]
            bikePathCoords.append(pathCoords)

        #Get Cambridge bike rack locations
        bikeracks = repo['cwsonn_levyjr.bikerack'].find()
        
        rackCoordData = []
        for c in bikeracks:
            coords = c["geometry"]["coordinates"]
            rackCoordData.append(coords)
                
        #Convert 2D Path Coord Array to 1D Array
        bikePathCoords.remove([[[-71.10018860972606, 42.373645069443846], [-71.10052894213501, 42.373689128835196], [-71.10083785435141, 42.37372911214897]], [[-71.10083740836019, 42.37372941244966], [-71.10123725248101, 42.37378068648357], [-71.10146445141548, 42.37381008939776]]])
        pathCoordData = []
        for i in range(len(bikePathCoords)):
            for j in range(len(bikePathCoords[i])):
                #Only get start and end points
                if(j == 0 or j == len(bikePathCoords)-1):
                    pathCoordData.append(bikePathCoords[i][j])

        #For each bike path start and end coordinate compare to each bike rack coordinate using the distance formula
        weights = []
        current_min = 1
        if(trial == True):
            for i in range(len(500)):
                for j in range(len(2600)):
                    #Compute distance
                    earthRadiusKm = 6371;
                    
                    lat1 = pathCoordData[i][1]
                    lon1 = pathCoordData[i][0]
                    lat2 = rackCoordData[j][1]
                    lon2 = rackCoordData[j][0]
                    
                    dLat = (lat2-lat1) * math.pi / 180
                    dLon = (lon2-lon1) * math.pi / 180
                    
                    lat1 = lat1 * math.pi / 180
                    lat2 = lat2 * math.pi / 180
                    
                    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
                    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                    x = earthRadiusKm * c
                    
                    if(x < current_min):
                        current_min = x
            
                #convert Km to m
                current_min *= 1000
                weights.append(current_min)
                current_min = 1
        else:
            for i in range(len(pathCoordData)):
                for j in range(len(rackCoordData)):
                    #Compute distance
                    earthRadiusKm = 6371;
            
                    lat1 = pathCoordData[i][1]
                    lon1 = pathCoordData[i][0]
                    lat2 = rackCoordData[j][1]
                    lon2 = rackCoordData[j][0]
                
                    dLat = (lat2-lat1) * math.pi / 180
                    dLon = (lon2-lon1) * math.pi / 180
                
                    lat1 = lat1 * math.pi / 180
                    lat2 = lat2 * math.pi / 180
                
                    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
                    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                    x = earthRadiusKm * c
                
                    if(x < current_min):
                        current_min = x
        
                #convert Km to m
                current_min *= 1000
                weights.append(current_min)
                current_min = 1

        sorted_weights = []
        #sort weights
        sorted_weights = sorted(weights)
        #Target higher distances
        sorted_weights.reverse()

        #Find the top hundred distances
        top_hundred = []
        #Add 500 new bike rack coordinates
        numRacks = 500
        for i in range(numRacks):
            top_hundred.append(sorted_weights[i])

        #Find those distances in original list
        index_lst = []
        for i in range(len(top_hundred)):
            index_lst.append(weights.index(top_hundred[i]))

        #Create new set of locations to add bike racks
        locations = []
        for i in range(len(index_lst)):
            locations.append(pathCoordData[i])
        
        #Add new rack locations to the database
        coordinatesToAddNewRacks = {'NewRackCoordinates': locations}
        repo['cwsonn_levyjr.bikeComparisonCam'].insert_one(coordinatesToAddNewRacks)

        #Add new locations to original list
        updatedRackList = []
        updatedRackList = rackCoordData + locations

        #Calculate the standard deviation of the distance weights
        stdWeights = statistics.stdev(weights)
        
        #For each bike path start and end coordinate compare to the updated bike rack coordinates that include the new rack locations using the distance formula
        updatedWeights = []
        current_min = 1
        for i in range(len(pathCoordData)):
            for j in range(len(updatedRackList)):
                #Compute distance
                earthRadiusKm = 6371;
                
                lat1 = pathCoordData[i][1]
                lon1 = pathCoordData[i][0]
                lat2 = updatedRackList[j][1]
                lon2 = updatedRackList[j][0]
                    
                dLat = (lat2-lat1) * math.pi / 180
                dLon = (lon2-lon1) * math.pi / 180
                
                lat1 = lat1 * math.pi / 180
                lat2 = lat2 * math.pi / 180
                                    
                a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                x = earthRadiusKm * c
                
                if(x < current_min):
                    current_min = x

            #convert Km to m
            current_min *= 1000
            updatedWeights.append(current_min)
            current_min = 1
        
        #Calculate the standard deviation of the distance weights
        stdWeights = statistics.stdev(weights)
        stdUpdatedWeights = statistics.stdev(updatedWeights)
        
        #Plot orginal distance weights
        plt.figure(1)
        plt.hist(weights, 20)
        plt.title("Distance Weights Between Racks and Paths. Standard Deviation = " + str(stdWeights))
        plt.xlabel("Distances (meters)")
        plt.ylabel("Frequency")
        plt.show()

        #Plot updated distance weights
        plt.figure(2)
        plt.hist(updatedWeights, 20)
        plt.title("Updated Distance Weights Between Racks and Paths. Standard Deviation = " + str(stdUpdatedWeights))
        plt.xlabel("Distances (meters)")
        plt.ylabel("Frequency")
        plt.show()
        
        #Compute distance from one bike rack to every other bike rack
        if(trial == False):
            racks = len(rackCoordData)
        else:
            racks = 1000
        distances = []
        for i in range(racks):
            for j in range(racks):
                earthRadiusKm = 6371;
                
                lat1 = rackCoordData[i][1]
                lon1 = rackCoordData[i][0]
                lat2 = rackCoordData[j][1]
                lon2 = rackCoordData[j][0]
                
                dLat = (lat2-lat1) * math.pi / 180
                dLon = (lon2-lon1) * math.pi / 180
                
                lat1 = lat1 * math.pi / 180
                lat2 = lat2 * math.pi / 180
                
                a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                x = earthRadiusKm * c
                x *= 1000
                distances.append(x)

        #Compute distance from one bike rack to every other bike rack in the updated list
        if(trial == False):
            racks = len(updatedRackList)
        else:
            racks = 1000
        updDistances = []
        for i in range(racks):
            for j in range(racks):
                earthRadiusKm = 6371;
                
                lat1 = updatedRackList[i][1]
                lon1 = updatedRackList[i][0]
                lat2 = updatedRackList[j][1]
                lon2 = updatedRackList[j][0]
                
                dLat = (lat2-lat1) * math.pi / 180
                dLon = (lon2-lon1) * math.pi / 180
                
                lat1 = lat1 * math.pi / 180
                lat2 = lat2 * math.pi / 180
                
                a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                x = earthRadiusKm * c
                x *= 1000
                updDistances.append(x)

        #Calculate standard deviation of distances
        stdDistances = statistics.stdev(distances)
        stdUpdDistances = statistics.stdev(updDistances)
        
        plt.figure(3)
        plt.hist(distances, 20)
        plt.title("Distances Between Bike Racks. Standard Deviation = " + str(stdDistances))
        plt.xlabel("Distances (meters)")
        plt.ylabel("Frequency")
        plt.show()
        
        plt.figure(4)
        plt.hist(updDistances, 20)
        plt.title("Updated Distances Between Bike Racks. Standard Deviation = " + str(stdUpdDistances))
        plt.xlabel("Distances (meters)")
        plt.ylabel("Frequency")
        plt.show()
        
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
        repo.authenticate('cwsonn_levyjr', 'cwsonn_levyjr')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log#')
        doc.add_namespace('bod', 'https://github.com/cambridgegis/cambridgegis_data/blob/master/Recreation/Bike_Facilities/RECREATION_BikeFacilities.geojson')
        doc.add_namespace('bod', 'https://github.com/cambridgegis/cambridgegis_data/blob/master/Recreation/Bike_Racks/RECREATION_BikeRacks.geojson')
        
        #Agent
        this_script = doc.agent('alg:bikeComparisonCam', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Resources
        bikerack = doc.entity('dat:bikerack', {'prov:label': 'bikerack', prov.model.PROV_TYPE:'ont:DataResource'})
        Cbikepath = doc.entity('dat:Cbikepath', {'prov:label': 'Cbikepath', prov.model.PROV_TYPE:'ont:DataResource'})
        
        #Activities
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        
        #Usage
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, bikerack, startTime)
        doc.used(this_run, Cbikepath, startTime)
        
        # New dataset
        bikeComparisonCam = doc.entity('dat:bikeComparisonCam', {prov.model.PROV_LABEL:'bikeComparisonCam',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeComparisonCam, this_script)
        doc.wasGeneratedBy(bikeComparisonCam, this_run, endTime)
        doc.wasDerivedFrom(bikeComparisonCam, bikerack, this_run, this_run, this_run)
        doc.wasDerivedFrom(bikeComparisonCam, Cbikepath, this_run, this_run, this_run)
        
        repo.logout()
        
        return doc

bikeConstraintSatisfaction.execute()
#doc = bikeComparisonCam.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


