import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import z3
import numpy as np
import math 
import numpy

#X = Longitude, Y = Latitude
def degreesToRadians(degrees):
    return degrees * math.pi / 180 

def distanceInKmBetweenEarthCoordinates(lat_1, lon_1, lat_2, lon_2):
    earthRadiusKm = 6371
    lat1 = float(lat_1)
    lat2 = float(lat_2)
    lon1 = float(lon_1)
    lon2 = float(lon_2)
    dLat = degreesToRadians(lat2-lat1)
    dLon = degreesToRadians(lon2-lon1)
    lat1 = degreesToRadians(lat1)
    lat2 = degreesToRadians(lat2)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return earthRadiusKm * c;

class statistics(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.newSchoolSubDis', 'debhe_shizhan0_wangdayu_xt.schoolHubwayDistance']
    writes = ['debhe_shizhan0_wangdayu_xt.statistics']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        
        # Loads the collection
        subway = []
        hubway = []
        #schools = repo['debhe_shizhan0_wangdayu_xt.allSchool'].find()
        subway = repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].find()
        hubway = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
        schoolToSubway = repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].find()

        # Need to make the deepcopy of record for further use
        subway_1 = copy.deepcopy(subway)
        #subway_2 = copy.deepcopy(subway)
        hubway_1 = copy.deepcopy(hubway)

        hubway_2 = copy.deepcopy(hubway)
        subway_2 = copy.deepcopy(subway)
        schoolToSubway_2 = copy.deepcopy(schoolToSubway)

        schoolToSubway_KM = []
        for row in schoolToSubway_2:
            schoolToSubway_KM.append( distanceInKmBetweenEarthCoordinates(row['schoolY'], row['schoolX'], row['subwayY'], row['subwayX']) )

        NewCalculation_AvgSchoolToSubway = sum(schoolToSubway_KM) / len(schoolToSubway_KM)
        print(len(schoolToSubway_KM))
        NewCalculation_StdevSchoolToSubway = numpy.std(schoolToSubway_KM)

        temp_list = []
        for row in hubway_2:
            #print(distanceInKmBetweenEarthCoordinates(row['School_Cor_y'], row['School_Cor_x'], row['Hubway_Cor_y'], row['Hubway_Cor_x']) )
            temp_list.append( distanceInKmBetweenEarthCoordinates(row['School_Cor_y'], row['School_Cor_x'], row['Hubway_Cor_y'], row['Hubway_Cor_x']) )

        NewCalculation_HubwayAvgDis_in_KM = sum(temp_list) / len(temp_list)
        #print("NewCalculation_HubwayAvgDis_in_KM:")
        #print(NewCalculation_HubwayAvgDis_in_KM)

        temp_list2 = []
        for row in subway_2:
            #print(distanceInKmBetweenEarthCoordinates(row['schoolY'], row['schoolX'], row['subwayY'], row['subwayX']))
            temp_list2.append( distanceInKmBetweenEarthCoordinates(row['schoolY'], row['schoolX'], row['subwayY'], row['subwayX']) )

        NewCalculation_BikeHubAvgDis_in_KM = sum(temp_list2) / len(temp_list2)
        #print("NewCalculation_BikeHubAvgDis_in_KM:")
        #print(NewCalculation_BikeHubAvgDis_in_KM)

        NewCalculation_HubwayStdevDis_in_KM = numpy.std(temp_list)
        #print("NewCalculation_HubwayStdevDis_in_KM")
        #print(NewCalculation_HubwayStdevDis_in_KM)

        NewCalculation_BikeHubStdevDis_in_KM = numpy.std(temp_list2)
        #print("NewCalculation_BikeHubStdevDis_in_KM")
        #print(NewCalculation_BikeHubStdevDis_in_KM)

        temp_list1_time = []
        for i in temp_list:
            temp_list1_time.append(i/0.083)

        temp_list2_time = []
        for i in temp_list2:
            temp_list2_time.append(i/0.66)

        temp_list2_time_walk = []
        for i in temp_list2:
            temp_list2_time_walk.append(i/0.083)

        schoolToSubway_time = []
        for i in schoolToSubway_KM:
            schoolToSubway_time.append(i/0.083)

        NewCalculation_AvgSchoolToSubwayTime = sum(schoolToSubway_time) / len(schoolToSubway_time)
        NewCalculation_StdevSchoolToSubwayTime = numpy.std(schoolToSubway_time)

        NewCalculation_HubbwayAvgTime_in_min = sum(temp_list1_time) / len(temp_list1_time)
        NewCalculation_BikeHubAveTime_in_min = sum(temp_list2_time) / len(temp_list2_time)
        NewCalculation_HubbwayStdevTime_in_min = numpy.std(temp_list1_time)
        NewCalculation_BikeHubStdevTime_in_min = numpy.std(temp_list2_time)

        NewCalculation_BikeHubAveTimeWalk_in_min = sum(temp_list2_time_walk) / len(temp_list2_time_walk)
        NewCalculation_BikeHubStdevTimeWalk_in_min = numpy.std(temp_list2_time_walk)

        subwaydist = []
        hubwaydist = []
        
        for i in subway_1:
            temp = i['Distance']
            subwaydist.append(temp)
            
        for j in hubway_1:
            temp = j['Distance']
            hubwaydist.append(temp)

        subwaydist_1 = copy.deepcopy(subwaydist)
        hubwaydist_1 = copy.deepcopy(hubwaydist)

        oldAssignmentAvg = 0
        newAssignmentAvg = 0

        # compute the average distance for the new assignment
        for i in subwaydist_1:
            newAssignmentAvg += i
        newAssignmentAvg = newAssignmentAvg / (len(subwaydist_1))

        # compute the average distance for the old assignment
        for i in hubwaydist_1:
            oldAssignmentAvg += i
        oldAssignmentAvg = oldAssignmentAvg / (len(hubwaydist_1))

        # Compute the covariance and correlation coeffision 
        compute = np.stack((subwaydist, hubwaydist), axis=0)
        cov = np.cov(compute)
        cor = np.corrcoef(compute)
        
        finalResult = {}
        finalResult['covariance'] = cov.tolist()
        finalResult['correlation'] = cor.tolist()
        finalResult['Old Assignment Average Distance'] = oldAssignmentAvg
        finalResult['New Assignment Average Distance'] = newAssignmentAvg
        
        finalResult['NewCalculation_HubwayAvgDis_in_KM (how far is a school to its closest hubway station)'] = NewCalculation_HubwayAvgDis_in_KM
        finalResult['NewCalculation_BikeHubAvgDis_in_KM (how far is a school to its closest new bike hub)'] = NewCalculation_BikeHubAvgDis_in_KM
        finalResult['NewCalculation_AvgSchoolToSubway (how far is a school to its closest subway station)'] = NewCalculation_AvgSchoolToSubway

        finalResult['NewCalculation_HubwayStdevDis_in_KM (stdev for school to its closest hubway station)'] = NewCalculation_HubwayStdevDis_in_KM
        finalResult['NewCalculation_BikeHubStdevDis_in_KM (stdev for school to its closest new bike hub)'] = NewCalculation_BikeHubStdevDis_in_KM
        finalResult['NewCalculation_StdevSchoolToSubway (Stdev for the distance of school to closest subway station)'] = NewCalculation_StdevSchoolToSubway

        finalResult['NewCalculation_HubbwayAvgTime_in_min (minutes takes to walk from school to cloest hubway station)'] = NewCalculation_HubbwayAvgTime_in_min
        finalResult['NewCalculation_BikeHubAveTimeWalk_in_min (minutes take to walk to nearest new bike hub)'] = NewCalculation_BikeHubAveTimeWalk_in_min
        finalResult['NewCalculation_HubbwayStdevTime_in_min (stdev for school to walk to hubway)'] = NewCalculation_HubbwayStdevTime_in_min
        finalResult['NewCalculation_BikeHubStdevTimeWalk_in_min (Stdev for time from school walk to new bike hub)'] = NewCalculation_BikeHubStdevTimeWalk_in_min
        
        finalResult['NewCalculation_BikeHubAveTime_in_min (minutes takes to bike to the closest subway station)'] = NewCalculation_BikeHubAveTime_in_min
        finalResult['NewCalculation_AvgSchoolToSubwayTime (minutes takes to walk from school to closest subway station)'] = NewCalculation_AvgSchoolToSubwayTime
        finalResult['NewCalculation_BikeHubStdevTime_in_min (stdev for school to ride to closest subway station)'] = NewCalculation_BikeHubStdevTime_in_min
        finalResult['NewCalculation_StdevSchoolToSubwayTime (Stdev for time from school walk to subway station)'] = NewCalculation_StdevSchoolToSubwayTime
        
        


        # save the information to the database
        repo.dropCollection("statistics")
        repo.createCollection("statistics")

        repo['debhe_shizhan0_wangdayu_xt.statistics'].insert(finalResult)
        repo['debhe_shizhan0_wangdayu_xt.statistics'].metadata({'complete': True})
        print("Saved statistics", repo['debhe_shizhan0_wangdayu_xt.statistics'].metadata())

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
        doc.add_namespace('bpd',
                          'http://bostonopendata-boston.opendata.arcgis.com/datasets/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        

        this_script = doc.agent('alg:#statistics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bpd:1d9509a8b2fd485d9ad471ba2fdb1f90_0',
                                             {'prov:label': 'The statistics result for our problem',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'csv'})

        get_statistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_statistics, this_script)
        doc.usage(get_statistics, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        statistics = doc.entity('dat:debhe_shizhan0_wangdayu_xt#statistics',
                          {prov.model.PROV_LABEL: 'The statistics result for our problem',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistics, this_script)
        doc.wasGeneratedBy(statistics, get_statistics, endTime)
        doc.wasDerivedFrom(statistics, resource, get_statistics, get_statistics, get_statistics)

        repo.logout()

        return doc
