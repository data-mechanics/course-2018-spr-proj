import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

class bostonClimate(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = []
    writes = ['aoconno8_dmak1112.bostonClimate']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        url = 'http://datamechanics.io/data/aoconno8_dmak1112/boston_climate_2015-2016.csv'
        df = pd.read_csv(url)
        new_df = df.filter(['DATE', 'REPORTTPYE', 'HOURLYDRYBULBTEMPF', 'HOURLYSKYCONDITIONS', 'HOURLYRelativeHumidity', 'HOURLYWindSpeed', 'HOURLYWindDirection', \
                        'HOURLYPrecip', 'DAILYMximumDryBulbTemp', 'DAILYMinimumDryBulbTemp', 'DAILYAverageDryBulbTemp', 'DAILYPrecip', 'DAILYWeather', \
                        'DAILYSnowfall','MonthlyMeanTemp', 'MonthlyTotalSnowfall','MonthlyTotalLiquidPrecip','MonthlyDaysWithLT32Temp',\
                        'MonthlyDaysWithLT0Temp'], axis=1)
        climate_dict = new_df.to_dict(orient='records')
        new_dict = {}
        for i in climate_dict:
            date = i['DATE']
            new_dict[date] = i

        final_dict = [new_dict]
        repo.dropCollection("bostonClimate")
        repo.createCollection("bostonClimate")
        repo['aoconno8_dmak1112.bostonClimate'].insert_many(final_dict)
        repo['aoconno8_dmak1112.bostonClimate'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112.bostonClimate'].metadata())

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
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aoconno8_dmak1112') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aoconno8_dmak1112') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:aoconno8_dmak1112#bostonClimate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:boston_climate_2015-2016', {'prov:label':'Boston Climate Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_bostonClimate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonClimate, this_script)
        doc.usage(get_bostonClimate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        bostonClimate = doc.entity('dat:aoconno8_dmak1112#bostonClimate', {prov.model.PROV_LABEL:'Boston Climate', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonClimate, this_script)
        doc.wasGeneratedBy(bostonClimate, get_bostonClimate, endTime)
        doc.wasDerivedFrom(bostonClimate, resource, get_bostonClimate, get_bostonClimate, get_bostonClimate)


        repo.logout()
                  
        return doc

# bostonClimate.execute()
# doc = bostonClimate.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
