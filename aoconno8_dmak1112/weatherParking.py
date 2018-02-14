import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

class weatherParking(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = ['aoconno8_dmak1112.bostonClimate', 'aoconno8_dmak1112.parkingData', 'aoconno8_dmak1112.yearlyEmissions']
    writes = ['aoconno8_dmak1112.weatherParking']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        parking = list(repo.aoconno8_dmak1112.parkingData.find())
        climate = list(repo.aoconno8_dmak1112.bostonClimate.find())
        emissions = list(repo.aoconno8_dmak1112.yearlyEmissions.find())

        eom_days = ['2015-01-31', '2015-02-28', '2015-03-31', '2015-04-30', '2015-05-31', '2015-06-30', '2015-07-31',\
                '2015-08-31', '2015-09-30', '2015-10-31', '2015-11-30', '2015-12-31']
        months = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September',\
                'October', 'November', 'December']


        # Parse the parking data to give us the total amount of cars parked in each zone for each month

        carsparked = {}
        bad_list = ['_id', 'GT by Zone', 'Zone Name', '#','August','September','October','November','December']
        month_list = []
        for i in parking[0]['result']['records']:
            temp = i.items()
            not_clean = weatherParking.select(temp, lambda t: t[0] not in bad_list)
            for j in range(len(not_clean)):
                if not_clean[j][1] is None:
                    temp_lst = [not_clean[j][0], not_clean[j][1]]
                    temp_lst[1] = 0
                    not_clean[j] = (temp_lst[0], temp_lst[1])
                elif not_clean[j][1] == '':
                    temp_lst = [not_clean[j][0], not_clean[j][1]]
                    temp_lst[1] = 0
                    not_clean[j] = (temp_lst[0], temp_lst[1])
                else:
                    temp_lst = [not_clean[j][0], not_clean[j][1]]
                    temp_lst[1] = int(temp_lst[1])
                    not_clean[j] = (temp_lst[0], temp_lst[1])
            month_list += not_clean


        #Aggregate the parking data to get the total amount of cars parked for each month
        all_months = dict(weatherParking.aggregate(month_list, sum))
        for i in all_months:
            temp_key = i
            temp_val = all_months[i]
            temp_dict = {}
            temp_dict['Cars Parked'] = temp_val
            carsparked[temp_key] = temp_dict



        #Parse the weather data to get the monthly temperature averages
        slim_dict = {}
        new_dict = climate[0]
        count = 0
        for i in new_dict:
            if count == 0:
                count+=1
                continue
            date = i

            var = ''
            if new_dict[i]['REPORTTPYE'] == 'SOD' and date[:10] in eom_days:
                for j in range(len(eom_days)):
                    if date[:10] == eom_days[j]:
                        var = months[j]
                        break
                slim_dict[var] = new_dict[i]
        final_climate_dict = {}
        for i in slim_dict:
            y = slim_dict[i].items()
            final_climate_dict[i] = dict(weatherParking.select(y,weatherParking.monthly_equals))

        final_climate_dict = final_climate_dict

        #Parse the weather data to get the vehicle emissions for 2015

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
        emissions_dict = emissions_dict



        #Combine the weather data and the parking data on the month so that we will have a key for the month,
        #and in that key we will have the monthly averages and the total number of cars parked for that month

        X = final_climate_dict.items()
        Y = carsparked.items()
        parking_weather = weatherParking.project(weatherParking.select(weatherParking.product(X,Y), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        for i in range(len(parking_weather)):
            weatherdict = (parking_weather[i][1])
            parkingdict = (parking_weather[i][2])
            weatherdict.update(parkingdict)
            parking_weather[i] = list(parking_weather[i])
            parking_weather[i] = parking_weather[i][:-1]
            parking_weather[i] = tuple(parking_weather[i])
        parking_weather = dict(parking_weather)

        #Combine the monthly weather/parking data with the emissions data into one list so we can insert it into MongoDB
        fancy_list = [0,0]
        fancy_list[1] = emissions_dict
        fancy_list[0] = parking_weather


        repo.dropCollection("weatherParking")
        repo.createCollection("weatherParking")
        repo['aoconno8_dmak1112.weatherParking'].insert_many(fancy_list)
        repo['aoconno8_dmak1112.weatherParking'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112.weatherParking'].metadata())
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

        this_script = doc.agent('alg:aoconno8_dmak1112#weatherParking', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        parking = doc.entity('dat:aoconno8_dmak1112#parkingData', {prov.model.PROV_LABEL:'Boston Parking Monthly 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        weather = doc.entity('dat:aoconno8_dmak1112#bostonClimate', {prov.model.PROV_LABEL:'Boston Climate', prov.model.PROV_TYPE:'ont:DataSet'})
        emissions = doc.entity('dat:aoconno8_dmak1112#yearlyEmissions', {prov.model.PROV_LABEL:'Yearly Emissions Data', prov.model.PROV_TYPE:'ont:DataSet'})

        
        get_weatherParking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_weatherParking, this_script)

        doc.usage(get_weatherParking, parking, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_weatherParking, weather, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_weatherParking, emissions, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation'
                }
                )

        weatherParking = doc.entity('dat:aoconno8_dmak1112#weatherParking', {prov.model.PROV_LABEL:'Monthly Parking with Monthly Weather and Emissions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(weatherParking, this_script)
        doc.wasGeneratedBy(weatherParking, get_weatherParking, endTime)
        doc.wasDerivedFrom(weatherParking, parking, get_weatherParking, get_weatherParking, get_weatherParking)
        doc.wasDerivedFrom(weatherParking, weather, get_weatherParking, get_weatherParking, get_weatherParking)
        doc.wasDerivedFrom(weatherParking, emissions, get_weatherParking, get_weatherParking, get_weatherParking)

 
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
        return [(t,u) for t in R for u in S]
    
    def monthly_equals(x):
        return ('Monthly' in x[0])
    
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]
# weatherParking.execute()
# doc = weatherParking.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
