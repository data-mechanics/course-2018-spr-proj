# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 02:04:32 2018

@author: Alexander
"""



import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from time import sleep
import sample

class yelpNYCTest(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.yelpNYCTest']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        
        collection = repo.bstc_semina.getNYCRestaurantData
        cursor = collection.find({})
        repo.dropCollection('yelpNYCTest')
        repo.createCollection('yelpNYCTest')
        for i in cursor:
            name = i["license_permit_holder"]
            address = i["address"] + " " + i["street"] + " " + i["borough"]
            r = sample.search(key,name,address)
            repo['bstc_semina.yelpTest'].insert_one(r)
        #url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=restaurants+in+boston&key=' + key
        #response = urllib.request.urlopen(url).read().decode()
#        response = response.replace("]", "")
#        response = response.replace("[", "")
#        response = "[" + response + "]"
        #r = json.loads(response)
        #pagetoken = r["next_page_token"]
        #print(len(r))
        #r = r["results"]
        #s = json.dumps(r, sort_keys=True, indent=2)
        #print(type(repo['bstc_semina.ApiTest']))
        repo['bstc_semina.yelpNYCTest'].metadata({'complete':True})
        #print(repo['bstc_semina.googleTest'].metadata())
        
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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc
    
yelpNYCTest.execute()
doc = yelpNYCTest.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
