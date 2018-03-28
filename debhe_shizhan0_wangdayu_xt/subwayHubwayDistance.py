import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

'''
This file will read the data in subwayStop table and hubwayStation table.
By using these two table, we are trying to calculate which hubway station 
is closest to a certain subway stop and what is their distance(by coordinates)
'''
class subwayHubwayDistance(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.subwayStop', 'debhe_shizhan0_wangdayu_xt.hubwayStation']
    writes = ['debhe_shizhan0_wangdayu_xt.subwayHubwayDistance']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        subwayStops = []
        bikeStations = []
        # Loads the collection
        subwayStops = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()
        bikeStations = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()

        #sub_l = subwayStops.count()

        #b_l = bikeStations.count()

        # finding the distance between each subway station and its closest hubway bike station
        # using product, projection, and selection
        minDistance = []
        
        # make a deepcopy of the data record
        subwayStops_1 = copy.deepcopy(subwayStops)
        subwayStops_2 = copy.deepcopy(subwayStops)
        allDistance = []
        temp = 0
        for row_1 in subwayStops_1:
            bikeStations_1 = copy.deepcopy(bikeStations)
            if(temp < 400):
                for row_2 in bikeStations_1:
                    s = (row_1['stopName'], row_2['station'], (float(row_1['X']) - float(row_2['X']))**2 + ( float(row_1['Y']) - float(row_2['Y']) )**2, row_2['dock_num'] )
                    allDistance.append(s)
            temp += 1
        # We are going to create a list that contains all the busStops that we are going to calculate as keys
        keys = []
        temp = 0
        for row in subwayStops_2:
            if(temp < 400):
                keys.append(row['stopName'])
            temp += 1
        
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
                        #print("Hello")
            minDistance.append({'stopName': stopName , 'hubwayStation': station, 'Distance': minD, 'numDock': num_dock})

        # save the data in the database
        repo.dropCollection("subwayHubwayDistance")
        repo.createCollection("subwayHubwayDistance")

        repo['debhe_shizhan0_wangdayu_xt.subwayHubwayDistance'].insert_many(minDistance)
        repo['debhe_shizhan0_wangdayu_xt.subwayHubwayDistance'].metadata({'complete': True})
        print("Saved subwayHubwayDistance", repo['debhe_shizhan0_wangdayu_xt.subwayHubwayDistance'].metadata())

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
        doc.add_namespace('dmo',
                          'http://datamechanics.io/data/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        

        this_script = doc.agent('alg:#subwayHubwayDistance',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dmo:MBTA_Stops',
                                             {'prov:label': 'minimum distance between subway stops and hubway',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'txt'})

        get_subwayHubwayDistance = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_subwayHubwayDistance, this_script)
        doc.usage(get_subwayHubwayDistance, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        subwayHubwayDistance = doc.entity('dat:debhe_shizhan0_wangdayu_xt#subwayHubwayDistance',
                          {prov.model.PROV_LABEL: 'minmium distance between subway stops and hubway',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(subwayHubwayDistance, this_script)
        doc.wasGeneratedBy(subwayHubwayDistance, get_subwayHubwayDistance, endTime)
        doc.wasDerivedFrom(subwayHubwayDistance, resource, get_subwayHubwayDistance, get_subwayHubwayDistance, get_subwayHubwayDistance)

        repo.logout()

        return doc
