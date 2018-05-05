import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This is the file that will combine the dataset from the database
It will read all the data from publicSchool table and privateSchool table;
filter out all the elementry schools and combine the rest data to form a new dataset. 
The new dataset will contain all the school(other than elementry schools) in Boston. 
'''
class allSchool(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.publicSchool', 'debhe_shizhan0_wangdayu_xt.privateSchool']
    writes = ['debhe_shizhan0_wangdayu_xt.allSchool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        # This is the helper function to access the database and get the data from the tables
        def getCollection(db):
        	temp = []
        	for i in repo['debhe_shizhan0_wangdayu_xt.' + db].find():
        		temp.append(i)
        	return temp

        # This is the helper function to union two datasets and form a new table
        def union(R, S):
        	return R + S

        # Get the data from the publicSchool table
        public_school_temp = getCollection('publicSchool')
        # Get the data from the privateSchool table
        private_school_temp = getCollection('privateSchool')

        '''
        this will filter out all the elementry schools from privateSchool table.
        All the school has grade lower than 6th grade will be filter out
        '''
        
        private_school = []
        for row in private_school_temp:
            if( ('6' in row['Grades']) or ('9' in row['Grades']) ) :
                private_school.append(row)

        '''
        this will filter out all the elementry schools from publicSchool table.
        All the other school will be remained.
        '''
        public_school = []
        for row in public_school_temp:
            if(row['School_type'] != 'ES'):
                public_school.append(row)

        # The union of two filtered data will be call allSchool
        # It will contain all the non-elementry school
        allSchool = []
        allSchool = union(public_school, private_school)

        # Create the table called allSchool and save the data in the database
        #repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')
        repo.dropCollection('debhe_shizhan0_wangdayu_xt.allSchool')
        repo.createCollection('debhe_shizhan0_wangdayu_xt.allSchool')
        repo['debhe_shizhan0_wangdayu_xt.allSchool'].insert_many(allSchool)
        repo['debhe_shizhan0_wangdayu_xt.allSchool'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.allSchool'].metadata())


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
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#allSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:0046426a3e4340a6b025ad52b41be70a_1', {'prov:label':'All School location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_allSchool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_allSchool, this_script)
        
        doc.usage(get_allSchool, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        allSchool = doc.entity('dat:debhe_shizhan0_wangdayu_xt#allSchool', {prov.model.PROV_LABEL:'All School location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(allSchool, this_script)
        doc.wasGeneratedBy(allSchool, get_allSchool, endTime)
        doc.wasDerivedFrom(allSchool, resource, get_allSchool, get_allSchool, get_allSchool)



        repo.logout()
                  
        return doc

#allSchool.execute()
#doc = allSchool.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))