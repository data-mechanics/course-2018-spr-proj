#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 17 22:42:56 2018

@author: paulahernandez
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class MBTA_Stops(dml.Algorithm):
    contributor = 'pandreah_xiangs18'
    reads = []
    writes = ['pandreah_xiangs18.MBTA_Stops']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()


        keep_list = ["Wonderland", "Revere Beach", "Beachmont", "Suffolk Downs", "Orient Heights", "Wood Island", "Airport", "Maverick", "Aquarium", "State", "Government Center", "Bowdoin", "Alewife", "Davis", "Porter", "Harvard", "Central", "Kendall/MIT", "Charles/MGH", "Park Street", "Downtown Crossing", "South Station", "Broadway", "Andrew", "JFK/Umass", "North Quincy", "Quincy Center", "Quincy Adams", "Braintree", "Savin Hill", "Fields Corner", "Shawmut", "Ashmont", "Oak Grove", "Malden Center", "Wellington", "Assembly", "Sullivan Square", "Community College", "North Station", "Haymarket", "State Street", "Downtown Crossing", "Chinatown", "Tufts Medical Center", "Back Bay", "Massachusetts Ave.", "Ruggles", "Roxbury Crossing", "Jackson Square", "Stony Brook", "Green Street", "Forest Hills", "Boston College", "South Street", "Chestnut Hill Ave.", "Chiswick Road", "Sutherland Road", "Washington Street", "Warrent Street", "Allston Street", "Griggs Street", "Harvard Avenue", "Packards Corner", "Babcock Street", "Pleasant Street", "Saint Paul Street", "Boston University West", "Boston University Central", "Boston University East", "Blandford Street", "Cleveland Circle", "Englewood Avenue","Dean Road", "Tappan Street", "Washington Square", "Fairbanks Street", "Brandon Hall", "Summit Avenue", "Coolidge Corner", "Saint Paul Street", "Kent Street", "Hawes Street", "Saint Marys Street", "Riverside", "Woodland", "Waban", "Eliot", "Newton Highlands", "Newton Centre", "Chestnut Hill", "Reservoir", "Beaconsfield", "Brookline Hills", "Brookline Village", "Longwood", "Fenway", "Kenmore", "Hynes Convention Center", "Heath Street", "Back of the Hill", "Riverway", "Mission Park", "Fenwood Road", "Brigham Circle", "Longwood Medical Area", "Museum of Fine Arts", "Northeastern University", "Symphony", "Prudential", "Copley", "Arlington", "Boylston", "Park Street", "Government Center", "Haymarket", "North Station", "Science Park", "Lechmere"]

# =============================================================================
#         num = 0
#         for stop in keep_list: 
#             num += 1
#             
#         print("This is the total number of t stops my data should have: ", num)
# =============================================================================

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah_xiangs18', 'pandreah_xiangs18')

        url = 'http://datamechanics.io/data/MBTA_Stops.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        clean_r = []
#        i = 0
        for row in r:
            if row['stop_id'] in keep_list:
#                i +=1
                clean_r.append(row)
            elif row['stop_name'] in keep_list:
#                i += 1
                clean_r.append(row)
#        print("This is the total of stops that my t data will have: ", i)
        s = json.dumps(clean_r, sort_keys=True, indent=2)
        repo.dropCollection("MBTA_Stops")
        repo.createCollection("MBTA_Stops")
        repo['pandreah_xiangs18.MBTA_Stops'].insert_many(clean_r)
        repo['pandreah_xiangs18.MBTA_Stops'].metadata({'complete':True})
        print(repo['pandreah_xiangs18.MBTA_Stops'].metadata())



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
        get_MBTA_Stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_MBTA_Stops, this_script)
        doc.usage(get_MBTA_Stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Hubway+Stations+Greater+Boston&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        MBTA_Stops = doc.entity('dat:pandreah_xiangs18#MBTA_Stops', {prov.model.PROV_LABEL:'All of the MBTA stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(MBTA_Stops, this_script)
        doc.wasGeneratedBy(MBTA_Stops, get_MBTA_Stops, endTime)
        doc.wasDerivedFrom(MBTA_Stops, resource, get_MBTA_Stops, get_MBTA_Stops, get_MBTA_Stops)
        repo.logout()
                  
        return doc
    
    

MBTA_Stops.execute()
doc = MBTA_Stops.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof