# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 00:55:17 2018

@author: Alexander
"""


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getInspectionData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getInspectionData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        url ='https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json'
        response = urllib.request.urlopen(url).read().decode()
        response = response.replace("]", "")
        response = response.replace("[", "")
        response = "[" + response + "]"
        r = json.loads(response)
        #print(len(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection('getInspectionData')
        repo.createCollection('getInspectionData')
        repo['bstc_semina.getInspectionData'].insert_many(r)
        #print(type(repo['bstc_semina.ApiTest']))
        repo['bstc_semina.getInspectionData'].metadata({'complete':True})
        print(repo['bstc_semina.getInspectionData'].metadata())
        
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

        this_script = doc.agent('alg:bstc_semina#getInspectionData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Health Inspections, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_inspect = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_inspect, this_script)
        doc.usage(get_inspect, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Health+Inspections&$select=businessName,DBAName,LegalOwner,NameLast,NameFirst,LICENSENO,ISSDTTM,EXPDTTM,LICSTATUS,LICENSECAT,DESCRIPT,RESULT,RESULTDTTM,Violation,ViolLevel,ViolDesc,VIOLDTTM,ViolStatus,StatusDate,Comments,Address,CITY,STATE,ZIP,Property_ID,Location'
                  }
                  )


        inspection = doc.entity('dat:bstc_semina#getInspectionData', {prov.model.PROV_LABEL:'Health Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(inspection, this_script)
        doc.wasGeneratedBy(inspection, get_inspect, endTime)
        doc.wasDerivedFrom(inspection, resource, get_inspect, get_inspect, get_inspect)


        repo.logout()
                  
        return doc
    
getInspectionData.execute()
doc = getInspectionData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))