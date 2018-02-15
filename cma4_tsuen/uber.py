import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

class uber(dml.Algorithm):
    contributor = 'cma4_tsuen'
    reads = []
    writes = ['cma4_tsuen.uber']
        
         

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_tsuen', 'cma4_tsuen')
        csvfile = open("./../data/boston-censustracts-2017-3-All-MonthlyAggregate.csv", 'r')
        jsonfile = open("./../data/uber.json", 'w')
 
         
        fieldnames = ["sourceid", "dstid", 'dow', 'mean_travel_time', 'standard_deviation_travel_time', 'geometric_mean_travel_time', 'geometric_standard_deviation_travel_time']
        reader = csv.DictReader(csvfile, fieldnames)
        l = []
        for row in reader:
            l.append(row)
        json.dump(l, jsonfile)
        url = 'http://datamechanics.io/?prefix=cma4_tsuen/uber.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.load(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_tsuen.uber")
        repo.createCollection("cma4_tsuen.uber")
        repo['cma4_tsuen.uber'].insert_many(r)
        repo['cma4_tsuen.uber'].metadata({'complete':True})
        print(repo['cma4_tsuen.uber'].metadata())

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
        doc.add_namespace('uber', 'https://movement.uber.com/curated/boston?lang=en-US')

        this_script = doc.agent('alg:cma4_tsuen#uber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('uber:travelTimes', {'prov:label':'Uber Travel Times Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_times = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_times, this_script)
        doc.usage(get_times, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        uber = doc.entity('dat:cma4_tsuen#uber', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber, this_script)
        doc.wasGeneratedBy(uber, get_times, endTime)
        doc.wasDerivedFrom(uber, resource, get_times, get_times, get_times)

        repo.logout()
                  
        return doc

#uber.PartToParts()
uber.execute()
#doc = uber.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof