import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class food(dml.Algorithm):
    contributor = 'cma4_tsuen'
    reads = []
    writes = ['cma4_tsuen.food']

    # def PartToParts():
    #     with open('./../data/food.json', 'r') as myfile:
            
    #         data=myfile.readlines()
            
    #         total = round(len(data) / 11)
    #         print(total)
    #         part_data = []
    #         current = 0
    #         for x in range(11):
    #             name = "part" + str(x+1)
    #             with open('./../data/food_' + name +'.txt', 'w') as partfile:
    #                 for y in range(current + 1, len(data)):
    #                     if(y % total == 0):
    #                         partfile.write(data[y])
    #                         current = y
    #                         break
    #                     partfile.write(data[y])
                     

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')            

        #url = 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json'
        food_data = []
        
        with open("./../data/food.txt", 'w') as partfile:
            for i in range(10):
                # url = 'http://datamechanics.io/data/cma4_tsuen/food_part' + str(i+1)+'.txt'
                # response = urllib.request.urlopen(url).read().decode("utf-8")
                
                # r = json.load(response)
                # print(r)
                # for y in range(len(r)):
                #    partfile.write(r[y])
        
                with open("./../data/food_part" + str(i+1) + ".json", 'r') as jsonfile:
                    data = jsonfile.readlines()
                    for y in range(len(data)):
                        partfile.write(data[y])
            

        with open("./../data/master_food.json", 'w') as finaljson:
            with open("./../data/food.txt", 'r') as jsonfile:
                data = jsonfile.readlines()
                for i in range(len(data)):
                    finaljson.write(data[i])

        jsonfile = open("./../data/master_food.json", 'r')
        r = json.load(jsonfile)
        
        #r = json.load(jsonfile)
        #food_data.append([{"Business Name": field['businessName'], "Coords": field['Location']}
        #for field in r])


        #jsonfile = open("./../data/food.json", 'r') 
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        #r = json.load(jsonfile)
        
        #filtered food.py
        
        print(food_data)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_tsuen.food")
        repo.createCollection("cma4_tsuen.food")
        repo['cma4_tsuen.food'].insert_many(r)
        repo['cma4_tsuen.food'].metadata({'complete':True})
        print(repo['cma4_tsuen.food'].metadata())

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
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('food', 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json')

        this_script = doc.agent('alg:cma4_tsuen#food', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:food', {'prov:label':'Food Locations Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_places = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_places, this_script)
        doc.usage(get_places, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        food = doc.entity('dat:cma4_tsuen#food', {prov.model.PROV_LABEL:'Food Places', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(food, this_script)
        doc.wasGeneratedBy(food, get_places, endTime)
        doc.wasDerivedFrom(food, resource, get_places, get_places, get_places)

        repo.logout()
                  
        return doc
#food.PartToParts()
food.execute()
#doc = food.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof