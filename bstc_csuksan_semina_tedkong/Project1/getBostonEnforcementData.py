# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 00:57:53 2018

@author: Alexander
- Building code violations in Boston
"""


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getBostonEnforcementData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getBostonEnforcementData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        url ='https://data.boston.gov/export/90e/d38/90ed3816-5e70-443c-803d-9a71f44470be.json'
        response = urllib.request.urlopen(url).read().decode()
        response = response.replace("]", "")
        response = response.replace("[", "")
        response = "[" + response + "]"
        r = json.loads(response)
        #print(len(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection('getBostonEnforcementData')
        repo.createCollection('getBostonEnforcementData')
        repo['bstc_semina.getBostonEnforcementData'].insert_many(r)
        #print(type(repo['bstc_semina.ApiTest']))
        repo['bstc_semina.getBostonEnforcementData'].metadata({'complete':True})
        print(repo['bstc_semina.getBostonEnforcementData'].metadata())
        
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bstc_semina') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/bstc_semina') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/90e/d38/')

        this_script = doc.agent('alg:bstc_semina#getBostonEnforcementData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:90ed3816-5e70-443c-803d-9a71f44470be', {'prov:label':'Building Code Violations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_enforce = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_enforce, this_script)
        doc.usage(get_enforce, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Building+Violations&$select=Ticket_No,Status_DTTM,Status,Code,Value,Description,StNo,StHigh,Street,Suffix,City,State,Zip,Property_ID,Latitude,Longitude,Location'
                  }
                  )

        enforcement = doc.entity('dat:bstc_semina#getBostonEnforcementData', {prov.model.PROV_LABEL:'Building Code Violations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(enforcement, this_script)
        doc.wasGeneratedBy(enforcement, get_enforce, endTime)
        doc.wasDerivedFrom(enforcement, resource, get_enforce, get_enforce, get_enforce)

        repo.logout()
                  
        return doc
    
getBostonEnforcementData.execute()
doc = getBostonEnforcementData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
