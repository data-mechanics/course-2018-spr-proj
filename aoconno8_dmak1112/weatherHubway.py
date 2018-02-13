import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

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

        parking = list(repo.aoconno8_dmak1112.hubwayTravel.find())
        climate = list(repo.aoconno8_dmak1112.bostonClimate.find())
        emissions = list(repo.aoconno8_dmak1112.yearlyEmissions.find())


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
                slim_dict[var] = new_dict[i]
        final_climate_dict = {}
        for i in slim_dict:
            y = slim_dict[i].items()
            final_climate_dict[i] = dict(weatherHubway.select(y,weatherHubway.daily_equals))
        print(len(final_climate_dict))
        final_climate_dict = [final_climate_dict]


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
        emissions_dict = [emissions_dict]

        # def product(R, S):
        #     return [(t, u) for t in R for u in S]

        # def select(R, s):
        #     return [t for t in R if s(t)]

        # def project(R, p):
        #     return [p(t) for t in R]

        # X = final_climate_dict.items()
        # parking_weather = project(select(product(X,Y), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        # for i in range(len(parking_weather)):
        #     weatherdict = (parking_weather[i][1])
        #     parkingdict = (parking_weather[i][2])
        #     weatherdict.update(parkingdict)
        #     parking_weather[i] = list(parking_weather[i])
        #     parking_weather[i] = parking_weather[i][:-1]
        #     parking_weather[i] = tuple(parking_weather[i])
        # parking_weather = dict(parking_weather)
        # fancy_list = [0,0]
        # fancy_list[1] = emissions_dict
        # fancy_list[0] = parking_weather





















        repo.dropCollection("weatherHubway")
        repo.createCollection("weatherHubway")
        repo['aoconno8_dmak1112.weatherHubway'].insert_many(final_climate_dict)


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
        pass
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('alice_bob', 'alice_bob')
        # doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        # doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        # doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # doc.wasAssociatedWith(get_found, this_script)
        # doc.wasAssociatedWith(get_lost, this_script)
        # doc.usage(get_found, resource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
        #           }
        #           )
        # doc.usage(get_lost, resource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #           }
        #           )

        # lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        # found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(found, this_script)
        # doc.wasGeneratedBy(found, get_found, endTime)
        # doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        # repo.logout()
                  
        # return doc

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
weatherHubway.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
