import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

class boston_assessing(dml.Algorithm):
    contributor = 'agoncharova_lmckone'
    reads = []
    writes = ['agoncharova_lmckone.boston_assessing_2014', 
    'agoncharova_lmckone.boston_assessing_2015',
    'agoncharova_lmckone.boston_assessing_2016',
    'agoncharova_lmckone.boston_assessing_2017']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Assessing Data from 2014-2017'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        #2014
        repo.dropCollection("boston_assessing_2014")
        repo.createCollection("boston_assessing_2014")
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=7190b0a4-30c4-44c5-911d-c34f60b22181'  
        response = ur.urlopen(url)
        data = json.load(response)
        r = data['result']['records']
        repo['agoncharova_lmckone.boston_assessing_2014'].insert_many(r)
        repo['agoncharova_lmckone.boston_assessing_2014'].metadata({'complete':True})
        
        #2015
        repo.dropCollection("boston_assessing_2015")
        repo.createCollection("boston_assessing_2015")
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=bdb17c2b-e9ab-44e4-a070-bf804a0e1a7f'  
        response = ur.urlopen(url)
        data = json.load(response)
        r = data['result']['records']
        repo['agoncharova_lmckone.boston_assessing_2015'].insert_many(r)
        repo['agoncharova_lmckone.boston_assessing_2015'].metadata({'complete':True})

        #2016
        repo.dropCollection("boston_assessing_2016")
        repo.createCollection("boston_assessing_2016")
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=cecdf003-9348-4ddb-94e1-673b63940bb8'  
        response = ur.urlopen(url)
        data = json.load(response)
        r = data['result']['records']
        repo['agoncharova_lmckone.boston_assessing_2016'].insert_many(r)
        repo['agoncharova_lmckone.boston_assessing_2016'].metadata({'complete':True})

        #2017
        repo.dropCollection("boston_assessing_2017")
        repo.createCollection("boston_assessing_2017")
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858'  
        response = ur.urlopen(url)
        data = json.load(response)
        r = data['result']['records']
        repo['agoncharova_lmckone.boston_assessing_2017'].insert_many(r)
        repo['agoncharova_lmckone.boston_assessing_2017'].metadata({'complete':True})
        print(repo['agoncharova_lmckone.boston_assessing_2017'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/')

        this_script = doc.agent('alg:agoncharova_lmckone#boston_assesing', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #2014
        resource_2014 = doc.entity('bdp:7190b0a4-30c4-44c5-911d-c34f60b22181', {'prov:label':'Boston Assessing 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_assessing_2014 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_assessing_2014, this_script)
        doc.usage(get_boston_assessing_2014, resource_2014, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        boston_assessing_2014 = doc.entity('dat:agoncharova_lmckone#boston_assessing_2014', {prov.model.PROV_LABEL:'Boston Assessing 2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_assessing_2014, this_script)
        doc.wasGeneratedBy(boston_assessing_2014, get_boston_assessing_2014, endTime)
        doc.wasDerivedFrom(boston_assessing_2014, resource_2014, get_boston_assessing_2014, get_boston_assessing_2014, get_boston_assessing_2014)
        
        #2015
        resource_2015 = doc.entity('bdp:bdb17c2b-e9ab-44e4-a070-bf804a0e1a7f', {'prov:label':'Boston Assessing 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_assessing_2015 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_assessing_2015, this_script)
        doc.usage(get_boston_assessing_2015, resource_2015, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        boston_assessing_2015 = doc.entity('dat:agoncharova_lmckone#boston_assessing_2015', {prov.model.PROV_LABEL:'Boston Assessing 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_assessing_2015, this_script)
        doc.wasGeneratedBy(boston_assessing_2015, get_boston_assessing_2015, endTime)
        doc.wasDerivedFrom(boston_assessing_2015, resource_2015, get_boston_assessing_2015, get_boston_assessing_2015, get_boston_assessing_2015)

        #2016
        resource_2016 = doc.entity('bdp:cecdf003-9348-4ddb-94e1-673b63940bb8', {'prov:label':'Boston Assessing 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_assessing_2016 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_assessing_2016, this_script)
        doc.usage(get_boston_assessing_2016, resource_2016, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        boston_assessing_2016 = doc.entity('dat:agoncharova_lmckone#boston_assessing_2016', {prov.model.PROV_LABEL:'Boston Assessing 2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_assessing_2016, this_script)
        doc.wasGeneratedBy(boston_assessing_2016, get_boston_assessing_2016, endTime)
        doc.wasDerivedFrom(boston_assessing_2016, resource_2016, get_boston_assessing_2016, get_boston_assessing_2016, get_boston_assessing_2016)
        
        #2017
        resource_2017 = doc.entity('bdp:062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label':'Boston Assessing 2017', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_assessing_2017 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_assessing_2017, this_script)
        doc.usage(get_boston_assessing_2017, resource_2017, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        boston_assessing_2017 = doc.entity('dat:agoncharova_lmckone#boston_assessing_2017', {prov.model.PROV_LABEL:'Boston Assessing 2017', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_assessing_2017, this_script)
        doc.wasGeneratedBy(boston_assessing_2017, get_boston_assessing_2017, endTime)
        doc.wasDerivedFrom(boston_assessing_2017, resource_2017, get_boston_assessing_2017, get_boston_assessing_2017, get_boston_assessing_2017)

        repo.logout()
                  
        return doc

#boston_assessing.execute()
#boston_assessing.provenance()

## eof