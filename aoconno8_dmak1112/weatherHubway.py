import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 
from dateutil.parser import parse
import numpy as np

class weatherHubway(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = ['aoconno8_dmak1112.bostonClimate', 'aoconno8_dmak1112.hubwayTravel', 'aoconno8_dmak1112.yearlyEmissions']
    writes = ['aoconno8_dmak1112.weatherHubway']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        hubway = list(repo.aoconno8_dmak1112.hubwayTravel.find())
        climate = list(repo.aoconno8_dmak1112.bostonClimate.find())
        emissions = list(repo.aoconno8_dmak1112.yearlyEmissions.find())






        ###############   HUBWAY  ###################


        
        holding_list = []
        is_unique = []
        gender_count = 0 
        for i in hubway:
            date_split = i['starttime'].split()
            try:
                year = int(i['birth year'])
            except:
                year = 1979
            if i['gender'] == 0:
                if gender_count %2 == 0:
                    gender = 1
                else:
                    gender = 2
            else:
                gender = i['gender']
            for_counting = 1
            readable = parse(date_split[0])
            date = readable.isoformat()
            tuples = tuple((date, i['tripduration'],year, gender, for_counting))
            holding_list.append(tuples)
            gender_count += 1




        avg_duration = weatherHubway.aggregate(weatherHubway.project(holding_list, lambda t: (t[0], t[1])), weatherHubway.mean )

        avg_year = weatherHubway.aggregate(weatherHubway.project(holding_list, lambda t: (t[0], t[2])), weatherHubway.mean)

        avg_gender = weatherHubway.aggregate(weatherHubway.project(holding_list, lambda t: (t[0], t[3])), weatherHubway.mean)

        total_daily_rides = weatherHubway.aggregate(weatherHubway.project(holding_list, lambda t: (t[0], t[4])), sum)


        hubway_dict = {}
        for i in range(len(avg_duration)):
            inner_dict = {}
            key = avg_duration[i][0]
            value = avg_duration[i][1]
            inner_dict['Average Duration'] = value
            hubway_dict[key] = inner_dict


        for i in hubway_dict:
            for j in range(len(avg_year)):
                if avg_year[j][0] == i:
                    hubway_dict[i]['Average Birth Year'] = avg_year[j][1]
                    break
            for k in range(len(avg_gender)):
                if avg_gender[k][0] == i:
                    hubway_dict[i]['Average Gender'] = avg_gender[k][1]
                    break
            for h in range(len(total_daily_rides)):
                if total_daily_rides[h][0] == i:
                    hubway_dict[i]['Total Number of Rides'] = total_daily_rides[h][1]
                    break

        print(len(hubway_dict))

        #############   HUBWAY  #####################



        
        
        #############   WEATHER  #####################

        slim_dict = {}
        new_dict = climate[0]
        count = 0 
        for i in new_dict:
            if count == 0:
                count+=1
                continue
            date = i

            if new_dict[i]['REPORTTPYE'] == 'SOD' and date[:4] == '2015':
                var = new_dict[i]['DATE'][:10]
                readable = parse(var)
                var = readable.isoformat()
                slim_dict[var] = new_dict[i]
        final_climate_dict = {}
        for i in slim_dict:
            y = slim_dict[i].items()
            final_climate_dict[i] = dict(weatherHubway.select(y,weatherHubway.daily_equals))

        #############   WEATHER  #####################


        #############   EMISSIONS  #####################

        emissions_dict = {}
        temp = emissions[0]['result']['records']
        var = ''
        for i in temp:
            if "Transportation" == i["Sector"] and "2015" == i["Year (Calendar Year)"]:
                var = i["Source"]
                emissions_dict[var] = i

        bad_list = ['Protocol', '_id', 'Source']
        for i in emissions_dict:
            for j in bad_list:
                emissions_dict[i].pop(j, 'None')

        #############   EMISSIONS  #####################




        #############   COMBINE ALL DATA  #####################

        X = final_climate_dict.items()
        Y = hubway_dict.items()
        hubway_weather = weatherHubway.project(weatherHubway.select(weatherHubway.product(X,Y), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

  
        for i in range(len(hubway_weather)):
            weatherdict = (hubway_weather[i][1])
            hubwaydict = (hubway_weather[i][2])
            weatherdict.update(hubwaydict)
            hubway_weather[i] = list(hubway_weather[i])
            hubway_weather[i] = hubway_weather[i][:-1]
            hubway_weather[i] = tuple(hubway_weather[i])
        hubway_weather = dict(hubway_weather)
        fancy_list = [0,0]
        fancy_list[1] = emissions_dict
        fancy_list[0] = hubway_weather







        repo.dropCollection("weatherHubway")
        repo.createCollection("weatherHubway")
        repo['aoconno8_dmak1112.weatherHubway'].insert_many(fancy_list)


        repo['aoconno8_dmak1112.weatherHubway'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112.weatherHubway'].metadata())

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

        this_script = doc.agent('alg:aoconno8_dmak1112#weatherHubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        hubway = doc.entity('dat:aoconno8_dmak1112#hubwayTravel', {prov.model.PROV_LABEL:'Hubway Daily Travel Data 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        weather = doc.entity('dat:aoconno8_dmak1112#bostonClimate', {prov.model.PROV_LABEL:'Boston Climate', prov.model.PROV_TYPE:'ont:DataSet'})
        emissions = doc.entity('dat:aoconno8_dmak1112#yearlyEmissions', {prov.model.PROV_LABEL:'Yearly Emissions Data', prov.model.PROV_TYPE:'ont:DataSet'})



        get_weatherHubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_weatherHubway, this_script)

        doc.usage(get_weatherHubway, hubway, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_weatherHubway, weather, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_weatherHubway, emissions, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation'
                }
                )

        weatherHubway = doc.entity('dat:aoconno8_dmak1112#weatherHubway', {prov.model.PROV_LABEL:'Daily Hubway with Daily Weather and Emissions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(weatherHubway, this_script)
        doc.wasGeneratedBy(weatherHubway, get_weatherHubway, endTime)
        doc.wasDerivedFrom(weatherHubway, hubway, get_weatherHubway, get_weatherHubway, get_weatherHubway)
        doc.wasDerivedFrom(weatherHubway, weather, get_weatherHubway, get_weatherHubway, get_weatherHubway)
        doc.wasDerivedFrom(weatherHubway, emissions, get_weatherHubway, get_weatherHubway, get_weatherHubway)

        repo.logout()
                  
        return doc


    #############   FOR TRANSFORMATIONS  #####################


    def union(R, S):
        return R + S

    def difference(R, S):
        return [t for t in R if t not in S]

    def intersect(R, S):
        return [t for t in R if t in S]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]
     
    def product(R, S):
        return [(t,u) for t in R for u in S]
    
    def daily_equals(x):
        return ('DAILY' in x[0])
    
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    def mean(x):
        return np.mean(x)

    #############   FOR TRANSFORMATIONS  #####################
weatherHubway.execute()
doc = weatherHubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
