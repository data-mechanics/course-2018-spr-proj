#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 17 19:22:26 2018

@author: paulahernandez
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hubwayStations(dml.Algorithm):
    contributor = 'pandreah_xiangs18'
    reads = []
    writes = ['pandreah_xiangs18.hubwayStations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah_xiangs18', 'pandreah_xiangs18')

        url = 'http://datamechanics.io/data/hubwaydump.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubwayStations")
        repo.createCollection("hubwayStations")
        repo['pandreah_xiangs18.hubwayStations'].insert_many(r)
        repo['pandreah_xiangs18.hubwayStations'].metadata({'complete':True})
        print(repo['pandreah_xiangs18.hubwayStations'].metadata())


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
        repo.authenticate('pandreah_xiangs18', 'pandreah_xiangs18')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pandreah_xiangs18#try1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hubwayStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubwayStations, this_script)

        doc.usage(get_hubwayStations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Hubway+Stations+Greater+Boston&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        hubwayStations = doc.entity('dat:pandreah_xiangs18#hubwayStations', {prov.model.PROV_LABEL:'Hubway Stations Greater Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayStations, this_script)
        doc.wasGeneratedBy(hubwayStations, get_hubwayStations, endTime)
        doc.wasDerivedFrom(hubwayStations, resource, get_hubwayStations, get_hubwayStations, get_hubwayStations)


        repo.logout()
                  
        return doc

hubwayStations.execute()
doc = hubwayStations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
