# Library data clean up 
import urllib.request
from urllib.request import quote 
import numpy as np
import math
import json
import csv
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveLibraries(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.libraries"] # change this reference everywhere else too

    @staticmethod
    def parseURL(url):
        return quote(url,safe='://*\'?=')
    
    @staticmethod
    def execute(trial = False):

        '''retrieve library dataset from city scores dataset'''
        
        startTime = datetime.datetime.now()

        # set up mongo database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah','bemullen_crussack_dharmesh_vinwah')
        

        ############ Filters entires for library metrics ########
        # https://s3-us-west-2.amazonaws.com/dsponsorascholar/591data/BosCityScore.json
        # url = RetrieveLibraries.parseURL('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "5bce8e71-5192-48c0-ab13-8faff8fef4d7" WHERE" CTY_SCR_NAME" = 'LIBRARY USERS' ''')
        # url = RetrieveLibraries.parseURL(''' https://s3-us-west-2.amazonaws.com/dsponsorascholar/591data/BosCityScore.json ''')
        
        url = 'https://s3-us-west-2.amazonaws.com/dsponsorascholar/591data/BosCityScore.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        #url = RetrieveLibraries.parseURL('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%225bce8e71-5192-48c0-ab13-8faff8fef4d7%22%20WHERE%20%22CTY_SCR_NAME%22%20=%20%27LIBRARY%20USERS%27%20 ''')
        
        #print(url)

        response = urllib.request.urlopen(url).read().decode("utf-8")
        #response = urllib.request.urlopen('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%225bce8e71-5192-48c0-ab13-8faff8fef4d7%22%20WHERE%20%22CTY_SCR_NAME%22%20=%20%27LIBRARY%20USERS%27%20 ''').read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r,sort_keys=True,indent=2)
        repo.dropCollection("libraries")
        repo.createCollection("libraries")
        repo['bemullen_crussack_dharmesh_vinwah.libraries'].insert_many(r)
        repo['bemullen_crussack_dharmesh_vinwah.libraries'].metadata({'complete':True})
        print(repo['bemullen_crussack_dharmesh_vinwah.libraries'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return{"start": startTime,"end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(),startTime= None,endTime=None):
        ''' 
            Create the provenance document describing everything happening in this 
            script. Each run of the script will generate a new document describing that
            innocation event.
            '''
         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        ###### this_script ##########
        
        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveLibraries',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        ###### rescource_libraries ##########
        
        resource_libraries = doc.entity('bdp:5bce8e71-5192-48c0-ab13-8faff8fef4d7',
            {'prov:label':'Libraries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        ####### get_libraries ##########
        
        get_libraries = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve Library Attendence metrics from CityScores dataset'})
        
        doc.wasAssociatedWith(get_libraries, this_script)

        doc.usage(get_libraries, resource_libraries, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': '?CITY_SCR_NAME=LIBRARY+USERS&$select=CITY_SCR_NAME,CTY_SCR_NBR_DY_01,CTY_SCR_NBR_WK_01,ETL_LOAD_DATE,CTY_SCR_WEEK,CTY_SCR_DAY'
                  })
        
        # what do i put for doc usage?

        libraries = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#Libraries', {prov.model.PROV_LABEL:'Libraries Metrics',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(libraries, this_script)
        doc.wasGeneratedBy(libraries, get_libraries, endTime)
        doc.wasDerivedFrom(libraries, resource_libraries, get_libraries,get_libraries, get_libraries)

        repo.logout()
                  
        return doc

RetrieveLibraries.execute()
doc = RetrieveLibraries.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

#from urllib import request



# ----------
# uncomment these to save CSV:
# ----------

# What I need to do is go through and make sure that I have this in the standard format with DML 
# (look at RetrieveCityScores.py between key =... and ...repo.logout(). Also, upload the ds non and student to 
# cspeople/dharmesh/cs591 so that they are up ! )
