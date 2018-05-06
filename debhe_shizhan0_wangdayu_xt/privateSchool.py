import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This is the file will get the data about private school
'''
class privateSchool(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = []
    writes = ['debhe_shizhan0_wangdayu_xt.privateSchool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        # Get the data from the following URL
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.csv'
        response = urllib.request.urlopen(url)
        # Parse the data to json and store it into database
        cr = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter = ',')
        ps = []
        i = 0
        for row in cr:
            if(i != 0):
                dic = {}
                dic['schoolName'] = row[5]
                dic['SCHID'] = row[4]
                dic['ADDRESS'] = row[6]
                dic['TOWN'] = row[8]
                dic['X'] = row[0]
                dic['Y'] = row[1]
                dic['Grades'] = row[14]
                ps.append(dic)
            i = i + 1

        # Connect to database and save the data into the collections
        repo.dropCollection("privateSchool")
        repo.createCollection("privateSchool")
        repo['debhe_shizhan0_wangdayu_xt.privateSchool'].insert_many(ps)
        repo['debhe_shizhan0_wangdayu_xt.privateSchool'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.privateSchool'].metadata())

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
        doc.add_namespace('bpb', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#privateSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bpb:0046426a3e4340a6b025ad52b41be70a_1', {'prov:label':'Private School Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_privateSchool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_privateSchool, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_privateSchool, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        privateSchool = doc.entity('dat:debhe_shizhan0_wangdayu_xt#privateSchool', {prov.model.PROV_LABEL:'private School', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(privateSchool, this_script)
        doc.wasGeneratedBy(privateSchool, get_privateSchool, endTime)
        doc.wasDerivedFrom(privateSchool, resource, get_privateSchool, get_privateSchool, get_privateSchool)



        repo.logout()
                  
        return doc

#privateSchool.execute()
#doc = privateSchool.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))