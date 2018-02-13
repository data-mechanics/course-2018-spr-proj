import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

class weatherParking(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = ['aoconno8_dmak1112.bostonClimate', 'aoconno8_dmak1112.parkingData']
    writes = ['aoconno8_dmak1112.weatherParking']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        parking = list(repo.aoconno8_dmak1112.parkingData.find())

        bad_list = ['_id', 'GT by Zone', 'Zone Name', '#']
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
        all_months = [dict(weatherParking.aggregate(month_list, sum))]
        print(all_months)
        repo.dropCollection("weatherParking")
        repo.createCollection("weatherParking")
        repo['aoconno8_dmak1112.weatherParking'].insert_many(all_months)
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

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]
weatherParking.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
