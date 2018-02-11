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

        csvfile = open("./../data/boston-censustracts-2017-3-WeeklyAggregate (1).csv", 'r')
        jsonfile = open("./../data/uber.json", 'r')

        """
        fieldnames = ["sourceid", "dstid", 'dow', 'mean_travel_time', 'standard_deviation_travel_time', 'geometric_mean_travel_time', 'geometric_standard_deviation_travel_time']
        reader = csv.DictReader(csvfile, fieldnames)
        l = []
        for row in reader:
            l.append(row)
        json.dump(l, jsonfile)
        """

        r = json.load(jsonfile)
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
        doc.add_namespace('uber', 'https://data.boston.gov/export/792/0c5/7920c501-b410-4a9c-85ab-51338c9b34af.json')

        this_script = doc.agent('alg:cma4_tsuen#uber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        lost = doc.entity('dat:cma4_tsuen#uber', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber, this_script)
        doc.wasGeneratedBy(uber, get_stops, endTime)
        doc.wasDerivedFrom(uber, resource, get_stops, get_stops, get_stops)

        repo.logout()
                  
        return doc

uber.execute()
doc = uber.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof