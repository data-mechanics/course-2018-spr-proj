import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt

class BostonRestaurantsScoreComparison(dml.Algorithm):
<<<<<<< HEAD
    
    contributor = "bstc_csuksan_semina_tedkong"
    reads = []
    writes = ['bstc_csuksan_semina_tedkong.BostonRestaurantsScoreComparison']
    
    
    
    
=======

    contributor = "bstc_csuksan_semina_tedkong"
    reads = []
    writes = ['bstc_csuksan_semina_tedkong.ScoreComparison_RatingAndViolation',
                'bstc_csuksan_semina_tedkong.ScoreComparison_Rating',
                'bstc_csuksan_semina_tedkong.ScoreComparison_Violation']

>>>>>>> aa11b4c5f2aa920f19d9789747119b29143f1074
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        """

        Read in data of edge weights

        """
        # file = pd.read_json("merged_datasets/BostonRestaurants_Map.json", lines=True)

        # new_collection_name = 'RestaurantRatingsAndHealthViolations_Boston'

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_csuksan_semina_tedkong', 'bstc_csuksan_semina_tedkong')

        collection_map = repo.bstc_csuksan_semina_tedkong.FullyConnectedMap
        cursor_map = collection_map.find({})

        file = pd.DataFrame(list(cursor_map))

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

        my_score_arr_combined = []       #array to store combined score to pd
        their_score_arr_combined = []    #array to store combined score to pd

        my_score_arr_rating = []        #array to store rating score to pd
        their_score_arr_rating = []     #array to store rating score to pd

        my_score_arr_violation = []     #array to store violation severity to pd
        their_score_arr_violation = []  #array to store violation severity to pd


        counter = 0
        for row in arr:
            ind = np.where(names == 'name')[0][0]
            ro = np.append(row[:ind], row[ind+1:]) #skip its own distance
            nam = np.append(names[:ind], names[ind+1:]) #skip its name. name is in format "name | unique identifer"

            z = zip(ro, nam) #map distance and name together
            z = list(z)
            z =  [(x, y) for x, y in z if y != '_id'] # remove _id field

            sort = sorted(z)[:6]
            their_score_combined = 0
            their_score_rating = 0
            their_score_violation = 0

            for i in sort:
                temp = i[1].split(' | ')
                temp = [s.replace('~','.') for s in temp]
                their_score_combined += float(temp[4]) + float(temp[3])
                their_score_rating += float(temp[4])
                their_score_violation += float(temp[3])

            temp = names[counter].split(' | ')
            temp = [s.replace('~','.') for s in temp]

            if(temp[0] == 'name'): #if found 'name', skip
                continue
            my_score_combined =  float(temp[4]) + float(temp[3])
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

<<<<<<< HEAD
        #Putting the scores comparison into dataframe                  
        df1 = pd.DataFrame({'My_Score':my_score_arr_combined, 'Their_Score':their_score_arr_combined})
        df2 = pd.DataFrame({'My_Score':my_score_arr_rating, 'Their_Score':their_score_arr_rating})
        df3 = pd.DataFrame({'My_Score':my_score_arr_violation, 'Their_Score':their_score_arr_violation})
        

        #printing and calculating the correlation
        corr1 = df1['My_Score'].corr(df1['Their_Score'])
        corr2 = df2['My_Score'].corr(df2['Their_Score'])
        corr3 = df3['My_Score'].corr(df3['Their_Score'])
        print(corr1)
        print(corr2)
        print(corr3)

        correlation = {"Correlation": [{"Score": "Rating + Violation Severity", "Correlation": corr1}, 
        {"Score": "Rating", "Correlation": corr2}, 
        {"Score": "Violation Severity", "Correlation": corr3} ] }

        with open('correlation_score.json', 'w') as fp:
            json.dump(correlation, fp)


        #print(correlation)
=======
        #need score, severity
        df1 = pd.DataFrame({'My_Score':my_score_arr_combined, 'Their_Score':their_score_arr_combined})
        df2 = pd.DataFrame({'My_Score':my_score_arr_rating, 'Their_Score':their_score_arr_rating})
        df3 = pd.DataFrame({'My_Score':my_score_arr_violation, 'Their_Score':their_score_arr_violation})
>>>>>>> aa11b4c5f2aa920f19d9789747119b29143f1074

        # df1.to_csv("CombinedScoreComparison.csv", encoding = 'utf-8')
        # df2.to_csv("RatingScoreComparison.csv", encoding = 'utf-8')
        # df3.to_csv("ViolationScoreComparison.csv", encoding = 'utf-8')

        new_collection_name = 'ScoreComparison_RatingAndViolation'
        repo.dropCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        repo.createCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        records = json.loads(df1.to_json(orient='records'))
        repo['bstc_csuksan_semina_tedkong.'+new_collection_name].insert_many(records)

        new_collection_name = 'ScoreComparison_Rating'
        repo.dropCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        repo.createCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        records = json.loads(df2.to_json(orient='records'))
        repo['bstc_csuksan_semina_tedkong.'+new_collection_name].insert_many(records)

        new_collection_name = 'ScoreComparison_Violation'
        repo.dropCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        repo.createCollection('bstc_csuksan_semina_tedkong.'+new_collection_name)
        records = json.loads(df3.to_json(orient='records'))
        repo['bstc_csuksan_semina_tedkong.'+new_collection_name].insert_many(records)

        """
        plt.figure(figsize=(12,12))
        plt.scatter(df1['Their_Score'], df1['My_Score'])

        plt.figure(figsize=(12,12))
        plt.scatter(df2['Their_Score'], df2['My_Score'])

        plt.figure(figsize=(12,12))
        plt.scatter(df3['Their_Score'], df3['My_Score'])
        """

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
        repo.authenticate('bstc_csuksan_semina_tedkong', 'bstc_csuksan_semina_tedkong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:bstc_csuksan_semina_tedkong#BostonRestaurantsScoreComparison', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
<<<<<<< HEAD
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
=======
        resource = doc.entity('dat:bstc_csuksan_semina_tedkong#FullyConnectedMap', {'prov:label':'Visualization', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
>>>>>>> aa11b4c5f2aa920f19d9789747119b29143f1074
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Data+Visuals&$select=name,distances'
                  }
                  )

<<<<<<< HEAD
        rate = doc.entity('dat:bstc_csuksan_semina_tedkong#BostonRestaurantsScoreComparison', {prov.model.PROV_LABEL:'Yelp Ratings', prov.model.PROV_TYPE:'ont:DataSet'})
=======
        rate = doc.entity('dat:bstc_csuksan_semina_tedkong#BostonRestaurantsScoreComparison', {prov.model.PROV_LABEL:'Data Visuals', prov.model.PROV_TYPE:'ont:DataSet'})
>>>>>>> aa11b4c5f2aa920f19d9789747119b29143f1074
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()

        return doc

BostonRestaurantsScoreComparison.execute()
doc = BostonRestaurantsScoreComparison.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
