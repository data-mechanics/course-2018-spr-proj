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
import matplotlib.pyplot as plt

class BostonRestaurantsScoreComparison(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.BostonRestaurantsScoreComparison']
    
    
    
    
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
        #print(names)
        file = file.sort_values(by='name')
        arr = np.array(file)
        top_connections = []
        
        """
        
        This for loops finds the 6 min value connections for each restaurant
        
        """
        
        my_score_arr_combined = []       #array to store combined score to pd
        their_score_arr_combined = []    #array to store combined score to pd

        my_score_arr_rating = []        #array to store rating score to pd
        their_score_arr_rating = []     #array to store rating score to pd

        my_score_arr_violation = []     #array to store violation severity to pd
        their_score_arr_violation = []  #array to store violation severity to pd


        counter = 0
        for row in arr:

            ind = np.where(names == 'name')[0][0]
            #na = row[ind]
            ro = np.append(row[:ind], row[ind+1:]) #skip its own distance
            nam = np.append(names[:ind], names[ind+1:]) #skip its name. name is in format "name | unique identifer"
           
            z = zip(ro, nam) #map distance and name together
            sort = sorted(z)[:6]
            their_score_combined = 0
            their_score_rating = 0
            their_score_violation = 0

            for i in sort: 
                temp = i[1].split(' | ')
                their_score_combined +=  float(temp[4]) + float(temp[3])
                their_score_rating += float(temp[4])
                their_score_violation += float(temp[3])

            my_temp = names[counter].split(' | ')
            
            if(my_temp[0] == 'name'): #if found 'name', skip
                continue
            my_score_combined =  float(my_temp[4]) + float(temp[3])
            my_score_rating = float(temp[4])
            my_score_violation = float(temp[3])

            their_score_combined = their_score_combined/6 #average out the score of weight severtity plus rating
            their_score_rating = their_score_rating/6
            their_score_violation = their_score_violation/6

            my_score_arr_combined.append(my_score_combined)
            their_score_arr_combined.append(their_score_combined)

            my_score_arr_rating.append(my_score_rating)
            their_score_arr_rating.append(their_score_rating)

            my_score_arr_violation.append(my_score_violation)
            their_score_arr_violation.append(their_score_violation)

            counter += 1
            #top_connections += [sort]

        #print(top_connections)  

        #need score, severity                  
        df1 = pd.DataFrame({'My_Score':my_score_arr_combined, 'Their_Score':their_score_arr_combined})
        df2 = pd.DataFrame({'My_Score':my_score_arr_rating, 'Their_Score':their_score_arr_rating})
        df3 = pd.DataFrame({'My_Score':my_score_arr_violation, 'Their_Score':their_score_arr_violation})
        #print(df)

        df1.to_csv("CombinedScoreComparison.csv", encoding = 'utf-8')
        df2.to_csv("RatingScoreComparison.csv", encoding = 'utf-8')
        df3.to_csv("VioletionScoreComparison.csv", encoding = 'utf-8')

        """
        plt.figure(figsize=(12,12))
        plt.scatter(df1['Their_Score'], df1['My_Score'])

        plt.figure(figsize=(12,12))
        plt.scatter(df2['Their_Score'], df2['My_Score'])

        plt.figure(figsize=(12,12))
        plt.scatter(df3['Their_Score'], df3['My_Score'])
        """
        
        file2 = pd.read_json("merged_datasets/RestaurantRatingsAndHealthViolations_Boston.json", lines=True)
        



        
        
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

        this_script = doc.agent('alg:bstc_semina#BostonRestaurantsScoreComparison', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Yelp+Reviews&$select=_id,businesses,total,region'
                  }
                  )

        rate = doc.entity('dat:bstc_semina#BostonRestaurantsScoreComparison', {prov.model.PROV_LABEL:'Yelp Ratings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()
                  
        return doc
    
BostonRestaurantsScoreComparison.execute()
doc = BostonRestaurantsScoreComparison.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
