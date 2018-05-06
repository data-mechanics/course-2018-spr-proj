import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

'''
This file will get the data from two different collections
It will get the data about restaurants and hubwayStation
It will find the closest hubway to each restaurants and
store it into the database
'''
class restaurantHubwayDistance(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.restaurants', 'debhe_shizhan0_wangdayu_xt.hubwayStation']
    writes = ['debhe_shizhan0_wangdayu_xt.restautantHubwayDistance']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        restaurant_3 = []
        bikeStations = []
        # Loads the collection
        restaurant_3 = repo['debhe_shizhan0_wangdayu_xt.restaurants'].find()
        bikeStations = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()

        # We are going to get rid of the restaurant that has no coordinate
        restaurant = []
        for row in restaurant_3:
            if(float(row['X']) != 0.0 and float(row['Y']) != 0.0):
                restaurant.append(row) 

        # finding the distance between each restaurant and its closest hubway bike station
        # using product, projection, and selection
        minDistance = []
        # make a deepcopy of the data list
        restaurant_1 = copy.deepcopy(restaurant)
        restaurant_2 = copy.deepcopy(restaurant)
        allDistance = []
        for row_1 in restaurant_1:
            bikeStations_1 = copy.deepcopy(bikeStations)
            for row_2 in bikeStations_1:
                s = (row_1['restaurantName'], row_2['station'], (float(row_1['X']) - float(row_2['X']))**2 + ( float(row_1['Y']) - float(row_2['Y']) )**2, row_2['dock_num'])
                allDistance.append(s)
        
        # Going to get a list of all the restaurant name as the key for further use
        keys = []
        for row in restaurant_2:
            keys.append(row['restaurantName'])

        for key in keys:
            minD = float('inf')
            restaurant = key
            station = ''
            for (k, b, v, d) in allDistance:
                if(key == k):
                    if(v < minD):
                        station = b
                        minD = v
                        num_dock = d
            minDistance.append({'restaurantName': restaurant , 'hubwayStation': station, 'Distance': minD, 'numDock': num_dock})

        # Connecting to the database and save the information in the collection
        repo.dropCollection("restaurantHubwayDistance")
        repo.createCollection("restaurantHubwayDistance")

        repo['debhe_shizhan0_wangdayu_xt.restaurantHubwayDistance'].insert_many(minDistance)
        repo['debhe_shizhan0_wangdayu_xt.restaurantHubwayDistance'].metadata({'complete': True})
        print("Saved restaurantHubwayDistance", repo['debhe_shizhan0_wangdayu_xt.restaurantHubwayDistance'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bsg',
                          'https://data.boston.gov/dataset/5e4182e3-ba1e-4511-88f8-08a70383e1b6/resource/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        

        this_script = doc.agent('alg:#restaurantHubwayDistance',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bsg:f1e13724-284d-478c-b8bc-ef042aa5b70b/download//licenses',
                                             {'prov:label': 'minimum distance between restaurant and hubway',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'csv'})

        get_restaurantHubwayDistance = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_restaurantHubwayDistance, this_script)
        doc.usage(get_restaurantHubwayDistance, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        restaurantHubwayDistance = doc.entity('dat:debhe_shizhan0_wangdayu_xt#restaurantHubwayDistance',
                          {prov.model.PROV_LABEL: 'minmium distance between restaurant and hubway',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(restaurantHubwayDistance, this_script)
        doc.wasGeneratedBy(restaurantHubwayDistance, get_restaurantHubwayDistance, endTime)
        doc.wasDerivedFrom(restaurantHubwayDistance, resource, get_restaurantHubwayDistance, get_restaurantHubwayDistance, get_restaurantHubwayDistance)

        repo.logout()

        return doc
