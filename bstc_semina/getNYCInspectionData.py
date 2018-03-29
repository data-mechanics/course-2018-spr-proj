# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 00:53:02 2018

@author: Alexander
"""



import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getNYCInspectionData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getNYCInspectionData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        
        app_token = dml.auth['services']['cityofnewyork']['token']
        
        url ='https://data.cityofnewyork.us/resource/xx67-kt59.json?$$app_token=' +app_token+ '&$limit=50000'
        response = urllib.request.urlopen(url).read().decode()
        #response = response.replace("]", "")
        #response = response.replace("[", "")
        #response = "[" + response + "]"
        r = json.loads(response)
        #print(len(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection('getNYCInspectionData')
        repo.createCollection('getNYCInspectionData')
        repo['bstc_semina.getNYCInspectionData'].insert_many(r)
        #print(type(repo['bstc_semina.ApiTest']))
        for i in range(8):
            url ='https://data.cityofnewyork.us/resource/xx67-kt59.json?$$app_token='+app_token+'&$limit=50000&$offset=' + str(50000 * i)
            response = urllib.request.urlopen(url).read().decode()
            r = json.loads(response)
            repo['bstc_semina.getNYCInspectionData'].insert_many(r)
        repo['bstc_semina.getNYCInspectionData'].metadata({'complete':True})
        #print(repo['bstc_semina.getNYCRestaurantData'].metadata())

        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return ({'start':startTime, 'end':endTime})

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bstc_semina#getRestaurantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Restaurants, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rest = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rest, this_script)
        doc.usage(get_rest, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Boston+Restaurants&$select=businessName,DBAName,Address,CITY,STATE,ZIP,LICSTATUS,LICENSECAT,DESCRIPT,licenseadddttm,dayphn,Property_ID,Location'
                  }
                  )


        restaurant = doc.entity('dat:bstc_semina#getRestaurantData', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurant, this_script)
        doc.wasGeneratedBy(restaurant, get_rest, endTime)
        doc.wasDerivedFrom(restaurant, resource, get_rest, get_rest, get_rest)


        repo.logout()
                  
        return doc
    
getNYCInspectionData.execute()
doc = getNYCInspectionData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))