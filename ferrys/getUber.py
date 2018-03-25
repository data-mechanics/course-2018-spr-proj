import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json
import urllib

class getUber(dml.Algorithm):
    contributor = 'ferrys'
    reads = []
    writes = ['ferrys.uber_travel', 'ferrys.uber_boundaries']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')

        url = 'http://datamechanics.io/data/ferrys/Uber_Travel_Times.csv'
        uber_travel_dict = pd.read_csv(url).to_dict(orient='records')
        
        repo.dropCollection("uber_travel")
        repo.createCollection("uber_travel")
        repo['ferrys.uber_travel'].insert_many(uber_travel_dict)
        repo['ferrys.uber_travel'].metadata({'complete':True})
        print(repo['ferrys.uber_travel'].metadata())
        
        url = 'http://datamechanics.io/data/ferrys/boston_censustracts.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        uber_boundaries = json.loads(response)['features']
        
        repo.dropCollection("uber_boundaries")
        repo.createCollection("uber_boundaries")
        repo['ferrys.uber_boundaries'].insert_many(uber_boundaries)
        repo['ferrys.uber_boundaries'].metadata({'complete':True})
        print(repo['ferrys.uber_boundaries'].metadata())
        

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
        repo.authenticate('ferrys', 'ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ferrys#getUber', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        uber_times_resource = doc.entity('dat:ferrys#Uber_Travel_Times', {'prov:label':'Uber Travel Times Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        boundaries_resource = doc.entity('dat:ferrys#boston_censustracts', {'prov:label':'Uber Boundary Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_uber = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_uber, this_script)
        doc.usage(get_uber, uber_times_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_uber, boundaries_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})



        uber_travel = doc.entity('dat:ferrys#uber_travel', {prov.model.PROV_LABEL:'uber_travel', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber_travel, this_script)
        doc.wasGeneratedBy(uber_travel, get_uber, endTime)
        doc.wasDerivedFrom(uber_travel, uber_times_resource, get_uber, get_uber, get_uber)
        
        uber_boundaries = doc.entity('dat:ferrys#uber_boundaries', {prov.model.PROV_LABEL:'uber_boundaries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(uber_boundaries, this_script)
        doc.wasGeneratedBy(uber_boundaries, get_uber, endTime)
        doc.wasDerivedFrom(uber_boundaries, boundaries_resource, get_uber, get_uber, get_uber)

        repo.logout()
                  
        return doc

#getUber.execute()
#doc = getUber.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

