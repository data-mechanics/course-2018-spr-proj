# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 21:52:43 2018

@author: Alexander
"""

import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
import json

class getBostonYelpRestaurantData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getBostonYelpRestaurantData']
    
    
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()        
                
        """
        
        Read in json data of edge weights
        
        """
        
        file = pd.read_json("merged_datasets/BostonRestaurants_Map.json", lines=True)
        
        
        """
        
        create array from dataframe
        sort dataframe
        
        """

        names = np.array(file.columns.values)
        file = file.sort_values(by='name')
        arr = np.array(file)
        top_connections = []
        
        """
        
        This for loops finds the 6 min value connections for each restaurant
        
        """
        
        
        for row in arr:
            ind = np.where(names == 'name')[0][0]
            #na = row[ind]
            ro = np.append(row[:ind], row[ind+1:])
            nam = np.append(names[:ind], names[ind+1:])
            z = zip(ro, nam)
            sort = sorted(z)[:6]
            top_connections += [sort]
        print(top_connections)                    
        
        
        file2 = pd.read_json("merged_datasets/RestaurantRatingsAndHealthViolations_Boston.json", lines=True)
        arr2 = file2[['ave_violation_severity', 'rating','latitude','longitude']].copy()
        
        
        
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
        repo.authenticate('bstc_semina', 'bstc_semina')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://www.yelp.com/developers/')

        this_script = doc.agent('alg:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Yelp+Reviews&$select=_id,businesses,total,region'
                  }
                  )

        rate = doc.entity('dat:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_LABEL:'Yelp Ratings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()
                  
        return doc
    
getBostonYelpRestaurantData.execute()
doc = getBostonYelpRestaurantData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
