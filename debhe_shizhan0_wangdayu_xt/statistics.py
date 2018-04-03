import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import z3
import numpy as np


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

        # Need to make the deepcopy of record for further use
        subway_1 = copy.deepcopy(subway)
        #subway_2 = copy.deepcopy(subway)
        hubway_1 = copy.deepcopy(hubway)

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
