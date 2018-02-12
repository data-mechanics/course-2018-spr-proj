# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 22:23:55 2018

@author: Alexander
"""



import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getLiquorData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getLiquorData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        url ='https://data.boston.gov/export/aab/353/aab353c1-c797-4053-a3fc-e893f5ccf547.json'
        response = urllib.request.urlopen(url).read().decode()
        response = response.replace("]", "")
        response = response.replace("[", "")
        response = "[" + response + "]"
        r = json.loads(response)
        #print(len(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection('getLiquorData')
        repo.createCollection('getLiquorData')
        repo['bstc_semina.getLiquorData'].insert_many(r)
        #print(type(repo['bstc_semina.ApiTest']))
        repo['bstc_semina.getLiquorData'].metadata({'complete':True})
        print(repo['bstc_semina.getLiquorData'].metadata())
        
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

        this_script = doc.agent('alg:bstc_semina#getEnforcementData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Building Code Violations, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_enforce = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_enforce, this_script)
        doc.usage(get_enforce, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Building+Violations&$select=Ticket_No,Status_DTTM,Status,Code,Value,Description,StNo,StHigh,Street,Suffix,City,State,Zip,Property_ID,Latitude,Longitude,Location'
                  }
                  )

        enforcement = doc.entity('dat:bstc_semina#getEnforcementData', {prov.model.PROV_LABEL:'Building Code Violations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(enforcement, this_script)
        doc.wasGeneratedBy(enforcement, get_enforce, endTime)
        doc.wasDerivedFrom(enforcement, resource, get_enforce, get_enforce, get_enforce)

        repo.logout()
                  
        return doc
    
getLiquorData.execute()
doc = getLiquorData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))