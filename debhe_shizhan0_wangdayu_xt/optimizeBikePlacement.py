import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import z3


class optimizeBikePlacement(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance', 'debhe_shizhan0_wangdayu_xt.subwayStop']
    writes = ['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        schoolDict = {}
        subwayDict = {}
        distanceHi = 0.002
        distanceLo = 0.000003
		
		# Loads the collection
        schools = []
        subwayStop = []
        #schools = repo['debhe_shizhan0_wangdayu_xt.allSchool'].find()
        schools = repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].find()
        subwayStop = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()

        # Need to make the deepcopy of record for further use
        schools_1 = copy.deepcopy(schools)
        schools_2 = copy.deepcopy(schools)
        schools_3 = copy.deepcopy(schools)
        listSchool = []

        totalDistance = 0
        maxLength = 0
        for i in schools_3:
            temp = i['Distance']
            totalDistance += temp
            if(temp > maxLength):
                maxLength = temp
        distanceLo = totalDistance / schools.count()
        distanceHi = maxLength + 0.000001
        print(distanceLo)

        for row_1 in schools_1:
            if(row_1['Distance'] < distanceLo):
                continue
            else:
                listSchool += [row_1['schoolName']]
                schoolDict[row_1['schoolName']] = []
                subwayStop_temp = copy.deepcopy(subwayStop)
                for row_2 in subwayStop_temp:
                    if(row_2['X'] != '""' and row_2['Y'] != '""' and row_1['schoolX'] != '""' and row_1['schoolY'] != '""'):
                        s = (row_1['schoolName'], row_2['stopName'], (float(row_1['schoolX']) - float(row_2['X']))**2 + ( float(row_1['schoolY']) - float(row_2['Y']) )**2, row_2['id'])
                        if(s[2] < distanceHi and s[2] > distanceLo):
                            schoolDict[row_1['schoolName']] += [row_2['stopName']]
                            


        #schoolDict_1 = copy.deepcopy(schoolDict)
        #print(len(schoolDict))
        solver = z3.Solver()
        allSchool = [[z3.Int(subStops) for subStops in schoolDict[s]] for s in listSchool]

        allSchool_1 = copy.deepcopy(allSchool)
        solver.add(sum([sum(school) for school in allSchool_1]) > 0)


        for school in allSchool:
            solver.add(sum(school) > 0)
            for subStops in school:
                solver.add(subStops < 2)
                solver.add(subStops >= 0)

        print(solver.check())
        placementResult = solver.model()
        #placementResult_1 = copy.deepcopy(placementResult)
        print(placementResult)
        print(len(placementResult))
        print(placementResult[placementResult[0]])

        finalResult = []
        assignCount = 0
        for i in range(len(placementResult)):
            dic = {}
            stopNameParsed = str(placementResult[i]).replace(".","")
            assignment = str(placementResult[placementResult[i]])
            dic[stopNameParsed] = assignment
            if(assignment == "1"):
                assignCount += 1
            finalResult.append(dic)
        print(assignCount)

        # save the information to the database
        repo.dropCollection("optimizeBikePlacement")
        repo.createCollection("optimizeBikePlacement")

        repo['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement'].insert_many(finalResult)
        repo['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement'].metadata({'complete': True})
        print("Saved optimizeBikePlacement", repo['debhe_shizhan0_wangdayu_xt.optimizeBikePlacement'].metadata())

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
        

        this_script = doc.agent('alg:#optimizeBikePlacement',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bpd:1d9509a8b2fd485d9ad471ba2fdb1f90_0',
                                             {'prov:label': 'minimum distance between school and Subway',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'csv'})

        get_optimizeBikePlacement = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_optimizeBikePlacement, this_script)
        doc.usage(get_optimizeBikePlacement, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        optimizeBikePlacement = doc.entity('dat:debhe_shizhan0_wangdayu_xt#optimizeBikePlacement',
                          {prov.model.PROV_LABEL: 'minmium distance between school and Subway',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimizeBikePlacement, this_script)
        doc.wasGeneratedBy(optimizeBikePlacement, get_optimizeBikePlacement, endTime)
        doc.wasDerivedFrom(optimizeBikePlacement, resource, get_optimizeBikePlacement, get_optimizeBikePlacement, get_optimizeBikePlacement)

        repo.logout()

        return doc
