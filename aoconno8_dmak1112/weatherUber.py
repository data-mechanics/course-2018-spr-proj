import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from dateutil.parser import parse

class weatherUber(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = ['aoconno8_dmak1112.bostonClimate', 'aoconno8_dmak1112.uberTravelTimes']
    writes = ['aoconno8_dmak1112.weatherUber']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')
        uber = list(repo.aoconno8_dmak1112.uberTravelTimes.find())
        climate = list(repo.aoconno8_dmak1112.bostonClimate.find())

        # Parse the uber data to only give us the date and the daily mean travel time
        uber_dict = uber
        good_data = []

        daily_travel_times = {}
        good_list = ['Date', 'Daily Mean Travel Time (Seconds)']
        for i in uber_dict:
            temp = i.items()
            not_clean = weatherUber.select(temp, lambda t: t[0] in good_list)

            if str(not_clean[1][1]) == "nan":
                pass
            else:
                not_clean = dict(not_clean)
                good_data.append(not_clean)

        for i in good_data:
            temp_key = i['Date']
            readable = parse(temp_key)
            temp_key = readable.isoformat()
            temp_val = i['Daily Mean Travel Time (Seconds)']
            temp_dict = {}
            temp_dict['Uber Daily Mean Travel Time'] = temp_val
            daily_travel_times[temp_key] = temp_dict




        # Parse the Weather data to only give us the date and the daily information from that day

        slim_dict = {}
        new_dict = climate[0]
        count = 0
        for i in new_dict:
            if count == 0:
                count += 1
                continue
            date = i


            if new_dict[i]['REPORTTPYE'] == 'SOD' and date[:4] == '2016':
                var = new_dict[i]['DATE'][:10]
                readable = parse(var)
                var = readable.isoformat()
                slim_dict[var] = new_dict[i]
        final_climate_dict = {}
        for i in slim_dict:
            y = slim_dict[i].items()
            final_climate_dict[i] = dict(weatherUber.select(y, weatherUber.daily_equals))


        #Combine the two datastes by date so that we have a date, and then the weather data and uber data for that date
        X = final_climate_dict.items()
        Y = daily_travel_times.items()
        weather_uber = weatherUber.project(weatherUber.select(weatherUber.product(X,Y), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        for i in range(len(weather_uber)):
            tempdict = (weather_uber[i][1])
            uberdict = (weather_uber[i][2])
            tempdict.update(uberdict)
            weather_uber[i] = list(weather_uber[i])
            weather_uber[i] = weather_uber[i][:-1]
            weather_uber[i] = tuple(weather_uber[i])
        weather_uber = dict(weather_uber)

        repo.dropCollection("weatherUber")
        repo.createCollection("weatherUber")
        repo['aoconno8_dmak1112.weatherUber'].insert_many([weather_uber])

        repo['aoconno8_dmak1112.weatherUber'].metadata({'complete': True})
        print(repo['aoconno8_dmak1112.weatherUber'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
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

        this_script = doc.agent('alg:aoconno8_dmak1112#weatherUber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        uber = doc.entity('dat:aoconno8_dmak1112#uberTravelTimes', {prov.model.PROV_LABEL:'Uber Travel Times', prov.model.PROV_TYPE:'ont:DataSet'})
        weather = doc.entity('dat:aoconno8_dmak1112#bostonClimate', {prov.model.PROV_LABEL:'Boston Climate', prov.model.PROV_TYPE:'ont:DataSet'})


        get_weatherUber = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_weatherUber, this_script)

        doc.usage(get_weatherUber, uber, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_weatherUber, weather, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        weatherUber = doc.entity('dat:aoconno8_dmak1112#weatherUber', {prov.model.PROV_LABEL:'Daily Weather and Uber Travel Times', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(weatherUber, this_script)
        doc.wasGeneratedBy(weatherUber, get_weatherUber, endTime)
        doc.wasDerivedFrom(weatherUber, uber, get_weatherUber, get_weatherUber, get_weatherUber)
        doc.wasDerivedFrom(weatherUber, weather, get_weatherUber, get_weatherUber, get_weatherUber)


        repo.logout()

        return doc

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
        return [(t, u) for t in R for u in S]

    def daily_equals(x):
        return ('DAILY' in x[0])

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]


# weatherUber.execute()
# doc = weatherUber.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
