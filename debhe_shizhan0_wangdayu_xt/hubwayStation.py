import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This file will get the date about hubway station, parse it and save it into the database
'''
class hubwayStation(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = []
    writes = ['debhe_shizhan0_wangdayu_xt.hubwayStation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        # Get the data from the following link
        url = 'https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv'
        response = urllib.request.urlopen(url)
        cr = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter = ',')

        # parse them as the json record
        ps = []
        i = 0
        for row in cr:
            if(i != 0):
                dic = {}
                dic['station_ID'] = row[0]
                dic['station'] = row[1]
                dic['X'] = row[3]
                dic['Y'] = row[2]
                dic['Municipality'] = row[4]
                dic['dock_num'] = row[5]
                ps.append(dic)
            i = i + 1

        # create the table and save the record to the database
        repo.dropCollection("debhe_shizhan0_wangdayu_xt.hubwayStation")
        repo.createCollection("debhe_shizhan0_wangdayu_xt.hubwayStation")
        repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].insert_many(ps)
        repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):


    # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('hbw', 'https://s3.amazonaws.com/hubway-data/')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#hubwayStation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('hbw:Hubway_Stations_as_of_July_2017', {'prov:label':'Hubway Station Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_hubwayStation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubwayStation, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_hubwayStation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        hubwayStation = doc.entity('dat:debhe_shizhan0_wangdayu_xt#hubwayStation', {prov.model.PROV_LABEL:'Hubway station location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayStation, this_script)
        doc.wasGeneratedBy(hubwayStation, get_hubwayStation, endTime)
        doc.wasDerivedFrom(hubwayStation, resource, get_hubwayStation, get_hubwayStation, get_hubwayStation)



        repo.logout()
                  
        return doc

#hubwayStation.execute()
#doc = hubwayStation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))