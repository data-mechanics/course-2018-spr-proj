import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta(dml.Algorithm):
    contributor = 'cma4_tsuen'
    reads = []
    writes = ['cma4_tsuen.mbta']


    def convertTxtToJSON():
        with open('./../data/MBTA_Stops.txt', 'r') as myfile:
            mbta_data = []
            data=myfile.readlines()
            for x in range(len(data)):
                line = data[x].split(",")
                
                if(line[4] != '""' and line[5] != '""'):
                    y = {'Stop_Name': line[2], 'Coords': (float(line[4]),float(line[5]))}
                mbta_data.append(y)  
            print(mbta_data)

        with open('./../data/mbta.json', 'w') as mbtafile:
          json.dump(mbta_data, mbtafile)
            
    def PartToParts():
        with open('./../data/food.json', 'r') as myfile:
            
            data=myfile.readlines()
            
            total = round(len(data) / 10)
            print(total)
            part_data = []
            current = 0
            for x in range(10):
                name = "part" + str(x+1)
                with open('./../data/food_' + name +'.json', 'w') as partfile:
                    for y in range(current + 1, len(data)+1):
                        if(y % total == 0):
                            partfile.write(data[y])
                            current = y
                            break
                        partfile.write(data[y])
    

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')

        url = 'http://datamechanics.io/data/cma4_tsuen/mbta.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_tsuen.mbta")
        repo.createCollection("cma4_tsuen.mbta")
        repo['cma4_tsuen.mbta'].insert_many(r)
        repo['cma4_tsuen.mbta'].metadata({'complete':True})
        print(repo['cma4_tsuen.mbta'].metadata())

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
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

        this_script = doc.agent('alg:cma4_tsuen#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbta:stops', {'prov:label':'MBTA Stops Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        mbta = doc.entity('dat:cma4_tsuen#mbta', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_stops, endTime)
        doc.wasDerivedFrom(mbta, resource, get_stops, get_stops, get_stops)

        repo.logout()
                  
        return doc
mbta.PartToParts()
#mbta.execute()
#doc = mbta.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
