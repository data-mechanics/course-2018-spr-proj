# Filename: retrieve.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Retrieve datasets from the sources provided and generate 
#              the data lineage.
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class Retrieve(dml.Algorithm):
    contributor = 'bemullen_dharmesh'
    reads = []
    writes = ['bemullen_dharmesh.cityscore', 'bemullen_dharmesh.lost', 'bemullen_dharmesh.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        # url = 'http://datamechanics.io/data/bemullen_dharmesh/data/311ServiceCalls.json'
        url = 'https://data.boston.gov/datastore/odata3.0/5bce8e71-5192-48c0-ab13-8faff8fef4d7?$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['value']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cityscore")
        repo.createCollection("cityscore")
        repo['bemullen_dharmesh.cityscore'].insert_many(r)
        repo['bemullen_dharmesh.cityscore'].metadata({'complete':True})
        print(repo['bemullen_dharmesh.cityscore'].metadata())

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
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')

        this_script = doc.agent('alg:bemullen_dharmesh#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdpm:5bce8e71-5192-48c0-ab13-8faff8fef4d7', {'prov:label':'CityScore', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cityscore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cityscore, this_script)
        doc.usage(get_cityscore, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$format=json'
                  }
                  )

        repo.logout()
                  
        return doc

# Retrieve.execute()
# doc = Retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
