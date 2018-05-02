import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import z3


class newSchoolSubDis(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement', 'debhe_shizhan0_wangdayu_xt.schoolSubwayDistance', 'debhe_shizhan0_wangdayu_xt.subwayStop']
    writes = ['debhe_shizhan0_wangdayu_xt.newSchoolSubDis']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        schoolDict = []
        subwayDict = {}
		
		# Loads the collection
        schools = []
        subwayStop = []
        #schools = repo['debhe_shizhan0_wangdayu_xt.allSchool'].find()
        bikePlacement = repo['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement'].find()
        schools = repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].find()
        subwayStop = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()

        # Need to make the deepcopy of record for further use
        schools_1 = copy.deepcopy(schools)
        schools_2 = copy.deepcopy(schools)
        schools_3 = copy.deepcopy(schools)
        listSchool = []

        # Find the average distance between school and its closest subway station
        # Find the maximum distance between school and its closest subway station
        totalDistance = 0
        maxLength = 0
        for i in schools_3:
            temp = i['Distance']
            totalDistance += temp
            if(temp > maxLength):
                maxLength = temp
        distanceLo = totalDistance / schools.count()
        distanceHi = maxLength + 0.000001
        #print(distanceLo)


        listSubway = {}
        for item in subwayStop:
            listSubway[item['stopName']] = (item['Y'], item['X'])

        # Find the list of all subway station that has the bike assignment by our algorithm
        listSubwayOfo = []
        for dic in bikePlacement:
            for subStop, assignment in dic.items():
                if (assignment == "1"):
                    listSubwayOfo += [subStop]
        #print(listSubwayOfo)
        # For each school if it is "close" to its closest subway station, it is not our target
        # for the bike hub station. So just add its distance to the closest subway station
        # For the other schools, find its distance to the closes station that get assigned 
        # a bike hub
        i = 0
        for row_1 in schools_1:
            schools_2[i]['schoolName'] = row_1['schoolName']
            schools_2[i]['schoolX'] = row_1['schoolX']
            schools_2[i]['schoolY'] = row_1['schoolY']
            if(row_1['Distance'] < distanceLo):
                schools_2[i]['subwayStation'] = row_1['subwayStation']
                schools_2[i]['Distance'] = row_1['Distance']
            else:
                minDis = float("inf")
                minSub = ''
                for subStop in listSubwayOfo:
                    temp_distance = (float(row_1['schoolX']) - float((listSubway[subStop])[1]))**2 + (float(row_1['schoolY']) - float((listSubway[subStop])[0]))**2
                    if (temp_distance < minDis):
                        minDis = temp_distance
                schools_2[i]['Distance'] = minDis
                schools_2[i]['subwayStation'] = minSub
            i += 1


        # save the information to the database
        repo.dropCollection("newSchoolSubDis")
        repo.createCollection("newSchoolSubDis")

        repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].insert_many(schools_2)
        repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].metadata({'complete': True})
        print("Saved newSchoolSubDis", repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].metadata())

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
        

        this_script = doc.agent('alg:#newSchoolSubDis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bpd:1d9509a8b2fd485d9ad471ba2fdb1f90_0',
                                             {'prov:label': 'find the new distance between school and new hub assignment',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'csv'})

        get_newSchoolSubDis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_newSchoolSubDis, this_script)
        doc.usage(get_newSchoolSubDis, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        newSchoolSubDis = doc.entity('dat:debhe_shizhan0_wangdayu_xt#newSchoolSubDis',
                          {prov.model.PROV_LABEL: 'find the new distance between school and new hub assignment',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(newSchoolSubDis, this_script)
        doc.wasGeneratedBy(newSchoolSubDis, get_newSchoolSubDis, endTime)
        doc.wasDerivedFrom(newSchoolSubDis, resource, get_newSchoolSubDis, get_newSchoolSubDis, get_newSchoolSubDis)

        repo.logout()

        return doc
