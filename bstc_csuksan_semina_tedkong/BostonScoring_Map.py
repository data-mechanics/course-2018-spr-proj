# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 11:17:29 2018

@author: Alexander
"""

import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
import json
import urllib.request

class BostonScoringMap(dml.Algorithm):

    contributor = "bstc_csuksan_semina_tedkong"
    reads = []
    writes = ['bstc_csuksan_semina_tedkong.BostonScoringMap']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        """

        First read in the merged Rating and Health Violation data and form a
        box around the city of Boston to cut out blatantly wrong data

        """

                
        urls = 'http://datamechanics.io/data/RestaurantRatingsAndHealthViolations_Boston.json'
        with urllib.request.urlopen(urls) as url:
            data = json.dumps(url.read().decode())
        temp = json.loads(data)
        file = pd.read_json(temp, lines=True)
        
        if(trial):
            splitted = np.array_split(file, 3)
            file = splitted[0]

        arr = file[['name', 'ave_violation_severity', 'rating','latitude','longitude']].copy()

        top = [42.400549, -71.004839]
        bot = [42.226935, -71.129931]
        right = [42.326260, -70.921191]
        left = [42.282590, -71.194209]

        arr = arr[(arr.latitude <= top[0]) & (arr.latitude >= bot[0]) & (arr.longitude <= right[1]) & (arr.longitude >= left[1])]
        arr = arr.drop_duplicates()
        arr = arr.sort_values(by='name')
        data = np.array(arr[['name', 'latitude','longitude', 'ave_violation_severity', 'rating']])

        print(data[:,1].mean(), data[:,1].max(), data[:,1].min())
        print(data[:,2].mean(), data[:,2].max(), data[:,2].min())
        print(data[:,3].mean(), data[:,3].max(), data[:,3].min())
        print(data[:,4].mean(), data[:,4].max(), data[:,4].min())

        """

        Create Dataframe that is a n^2 array of all connections

        """

        distance = pd.DataFrame()

        for i in data:
            val = {'name':i[0]}
            val.update({'latitude' : i[1]})
            val.update({'longitude' : i[2]})
            score = (0.01 * i[3]) + (0.01 * i[4]) / 2
            val.update({'score' : score})
            di = pd.DataFrame([val])
            distance = pd.concat([distance, di])

        ## write to json file
        js = distance.to_json(orient='records')[:].replace('},{', '}\n{')
        js = js.replace('[','')
        js = js.replace(']','')
        with open("BostonScoring_Map.json", 'w') as jf:
            jf.write(js)

        ## TODO: insert into mongodb
#        repo.logout()

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
        repo.authenticate('bstc_csuksan_semina_tedkong', 'bstc_csuksan_semina_tedkong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:bstc_csuksan_semina_tedkong#BostonScoringMap', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:bstc_csuksan_semina_tedkon#dat:RestaurantRatingsAndHealthViolations_Boston', {'prov:label':'Score Map', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Ratings+Health+Violations&$select=name,ave_violation_severity,rating,latitude,longitude'
                  }
                  )

        rate = doc.entity('dat:bstc_csuksan_semina_tedkong#BostonScoringMap', {prov.model.PROV_LABEL:'Score Map', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()

        return doc

BostonScoringMap.execute()
doc = BostonScoringMap.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
