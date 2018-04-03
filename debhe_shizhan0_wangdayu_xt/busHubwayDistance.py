import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

'''
This file will read the data in busStop table and hubwayStation table.
By using these two table, we are trying to calculate which hubway station 
is closest to a certain bus stop and what is their distance(by coordinates)
'''
class busHubwayDistance(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.busStop', 'debhe_shizhan0_wangdayu_xt.hubwayStation']
    writes = ['debhe_shizhan0_wangdayu_xt.busHubwayDistance']

    
    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        
        
        # Loads the busStop collection
        busStops = []
        busStops = repo['debhe_shizhan0_wangdayu_xt.busStop'].find()
        # Loads the hubwayStation collection
        bikeStations = []
        bikeStations = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()

        # Make a deep copy of our data for further use
        busStops_1 = copy.deepcopy(busStops)
        busStops_2 = copy.deepcopy(busStops)

        # We are going to do the product of busStop collection and hubwayStation collection
        # When we are doing that, we are going to filter out all the un-useful station
        # We are going to record the bus Stop, the hubway stop and the distance between them
        # There are too much busStops in Boston, so we are going to calculate the first 2000 of them
        allDistance = []
        temp = 0
        if(trial == true):
            for row_1 in busStops_1:
                bikeStations_1 = copy.deepcopy(bikeStations)
                if(temp < 1000):
                    for row_2 in bikeStations_1:
                        s = (row_1['stopName'], row_2['station'], (float(row_1['X']) - float(row_2['X']))**2 + ( float(row_1['Y']) - float(row_2['Y']) )**2, row_2['dock_num'])
                        allDistance.append(s)
                temp += 1
        else:
            for row_1 in busStop_1:
                bikeStations_1 = copy.deepcopy(bikeStations)
                for row_2 in bikeStations_1:
                    s = (row_1['stopName'], row_2['station'], (float(row_1['X']) - float(row_2['X']))**2 + ( float(row_1['Y']) - float(row_2['Y']) )**2, row_2['dock_num'])
                    allDistance.append(s)
                temp += 1
                
        # We are going to create a list that contains all the busStops that we are going to calculate as keys
        keys = []
        temp = 0
        for row in busStops_2:
            if(temp < 1000):
                keys.append(row['stopName'])
            temp += 1

        # Find the closest pair between busStop and hubwayStation and record them
        # using product, projection, and selection
        minDistance = []
        for key in keys:
            minD = float('inf')
            stopName = key
            station = ''
            for (k, b, v, d) in allDistance:
                if(key == k):
                    if(v < minD):
                        station = b
                        minD = v
                        num_dock = d
            minDistance.append({'stopName': stopName , 'hubwayStation': station, 'Distance': minD, 'numDock': num_dock})

        # We are going to create a table and save the records that we just calculated.
        repo.dropCollection("busHubwayDistance")
        repo.createCollection("busHubwayDistance")

        repo['debhe_shizhan0_wangdayu_xt.busHubwayDistance'].insert_many(minDistance)
        repo['debhe_shizhan0_wangdayu_xt.busHubwayDistance'].metadata({'complete': True})
        print("Saved busHubwayDistance", repo['debhe_shizhan0_wangdayu_xt.busHubwayDistance'].metadata())

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
        doc.add_namespace('ont',
                          'http://datamechanics.io/data/wuhaoyu_yiran123/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#busHubwayDistance',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('ont:MBTA_Bus_Stops',
                                             {'prov:label': 'minimum distance between bus stops and hubway',
                                              prov.model.PROV_TYPE: 'geojson'})

        get_busHubwayDistance = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_busHubwayDistance, this_script)
        doc.usage(get_busHubwayDistance, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        busHubwayDistance = doc.entity('dat:debhe_shizhan0_wangdayu_xt#busHubwayDistance',
                          {prov.model.PROV_LABEL: 'minmium distance between bus stops and hubway',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(busHubwayDistance, this_script)
        doc.wasGeneratedBy(busHubwayDistance, get_busHubwayDistance, endTime)
        doc.wasDerivedFrom(busHubwayDistance, resource, get_busHubwayDistance, get_busHubwayDistance, get_busHubwayDistance)

        repo.logout()

        return doc
