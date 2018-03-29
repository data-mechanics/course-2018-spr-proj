import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This file will get the data, parse them and
store that into database
'''
class subwayStop(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = []
    writes = ['debhe_shizhan0_wangdayu_xt.subwayStop']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        # Get the data from the following source
        url = 'http://datamechanics.io/data/MBTA_Stops.txt'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        data = response.split("\n")

        # parse the data
        subStop = []
        i = 0
        for line in data:
            if(i != 0):
                line = line.split(',')
                dic = {}
                dic['stopName'] = line[2][2:-2]
                dic['Y'] = line[4]
                dic['X'] = line[5]
                dic['id'] = i
                subStop.append(dic)
            i = i + 1

        # connect to the database and store the data
        repo.dropCollection("subwayStop")
        repo.createCollection("subwayStop")
        repo['debhe_shizhan0_wangdayu_xt.subwayStop'].insert_many(subStop)
        repo['debhe_shizhan0_wangdayu_xt.subwayStop'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.subwayStop'].metadata())

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):


    # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dmo', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#subwayStop', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dmo:MBTA_Stops', {'prov:label':'Public School Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_subwayStop = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_subwayStop, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_subwayStop, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        subwayStop = doc.entity('dat:debhe_shizhan0_wangdayu_xt#subwayStop', {prov.model.PROV_LABEL:'MBTA subway stop', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(subwayStop, this_script)
        doc.wasGeneratedBy(subwayStop, get_subwayStop, endTime)
        doc.wasDerivedFrom(subwayStop, resource, get_subwayStop, get_subwayStop, get_subwayStop)



        repo.logout()
                  
        return doc

#subwayStop.execute()
#doc = subwayStop.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))