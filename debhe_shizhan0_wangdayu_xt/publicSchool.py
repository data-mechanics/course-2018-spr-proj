import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This file will get the data from open source database and 
parse them then put into the database
'''
class publicSchool(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = []
    writes = ['debhe_shizhan0_wangdayu_xt.publicSchool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        # the data will get from the following link
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        response = urllib.request.urlopen(url)
        cr = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter = ',')
        # parse the data as following
        ps = []
        i = 0
        for row in cr:
            if(i != 0):
                dic = {}
                dic['schoolName'] = row[11]
                dic['bldg'] = row[3]
                dic['sch_id'] = row[9]
                dic['zipcode'] = row[7]
                dic['X'] = row[0]
                dic['Y'] = row[1]
                dic['School_type']= row[12]
                ps.append(dic)
            i = i + 1


        repo.dropCollection("publicSchool")
        repo.createCollection("publicSchool")
        repo['debhe_shizhan0_wangdayu_xt.publicSchool'].insert_many(ps)
        repo['debhe_shizhan0_wangdayu_xt.publicSchool'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.publicSchool'].metadata())

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
        doc.add_namespace('bpd', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#publicSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bpd:1d9509a8b2fd485d9ad471ba2fdb1f90_0', {'prov:label':'Public School Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_publicSchool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_publicSchool, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_publicSchool, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        publicSchool = doc.entity('dat:debhe_shizhan0_wangdayu_xt#publicSchool', {prov.model.PROV_LABEL:'pulic School', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(publicSchool, this_script)
        doc.wasGeneratedBy(publicSchool, get_publicSchool, endTime)
        doc.wasDerivedFrom(publicSchool, resource, get_publicSchool, get_publicSchool, get_publicSchool)



        repo.logout()
                  
        return doc

#publicSchool.execute()
#doc = publicSchool.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))