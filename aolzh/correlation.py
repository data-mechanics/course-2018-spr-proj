import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats
import random
from vincenty import vincenty

class correlation(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkNormHouses']
    writes = ['aolzh.Correlation']

    @staticmethod
    def execute(trial = False):
        print("correlation")
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorknormhouses = repo.aolzh.NewYorkNormHouses
        
        
        houses = newyorknormhouses.find()

        houses_correlation = []

        houses_score = []

        norm_rate = []
        norm_stores = []
        norm_school = []
        norm_crime = []
        norm_subway = []
        norm_hospitals = []

        for h in houses:
            norm_school.append(h['norm_school'])
            norm_crime.append(h['norm_crime'])
            norm_subway.append(h['norm_subway'])
            norm_hospitals.append(h['norm_hospitals'])
            norm_stores.append(h['norm_stores'])
            norm_rate.append(h['norm_rate'])
            # 30% price 20% crime 20% subway 10% school 10% stores 10% hospitals
            houses_score.append(h['norm_rate']*0.3+ h['norm_crime']*0.2 + h['norm_subway']*0.2 + h['norm_school']*0.1 + h['norm_stores']*0.1 + h['norm_hospitals']*0.1)
        

        crime_score = scipy.stats.pearsonr(houses_score, norm_crime)
        subway_score = scipy.stats.pearsonr(houses_score, norm_subway)
        school_score = scipy.stats.pearsonr(houses_score, norm_school)
        hospitals_score = scipy.stats.pearsonr(houses_score, norm_hospitals)
        stores_score = scipy.stats.pearsonr(houses_score, norm_stores)
        rate_score = scipy.stats.pearsonr(houses_score, norm_rate)

        print(crime_score)
        print(subway_score)
        print(school_score)
        print(hospitals_score)
        print(stores_score)
        print(rate_score)

        houses_correlation.append({'crime_coefficient':crime_score[0], 'crime_pvalue':crime_score[1]})
        houses_correlation.append({'subway_coefficient':subway_score[0], 'subway_pvalue':subway_score[1]})
        houses_correlation.append({'school_coefficient':school_score[0], 'school_pvalue':school_score[1]})
        houses_correlation.append({'hospitals_coefficient':hospitals_score[0], 'hospitals_pvalue':hospitals_score[1]})
        houses_correlation.append({'stores_coefficient':stores_score[0], 'stores_pvalue':stores_score[1]})
        houses_correlation.append({'rate_coefficient':rate_score[0], 'rate_pvalue':rate_score[1]})

        repo.dropCollection("Correlation")
        repo.createCollection("Correlation")
        repo["aolzh.Correlation"].insert_many(houses_correlation)
        print("Finished")

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
        repo.authenticate('aolzh', 'aolzh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        correlation_script = doc.agent('alg:aolzh#correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorknormhouses_resource = doc.entity('dat:aolzh#NewYorkNormHouses', {prov.model.PROV_LABEL:'NewYork Norm Houses', prov.model.PROV_TYPE:'ont:DataSet'})

        correlation_ = doc.entity('dat:aolzh#Correlation', {prov.model.PROV_LABEL:'NewYork Houses Correlation', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(get_correlation, correlation_script)

        doc.usage(get_correlation, newyorknormhouses_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        doc.wasAttributedTo(correlation_, correlation_script)
        doc.wasGeneratedBy(correlation_, get_correlation, endTime)
        doc.wasDerivedFrom(correlation_, newyorknormhouses_resource,get_correlation, get_correlation, get_correlation)

        repo.logout()
                  
        return doc

correlation.execute()
doc = correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
