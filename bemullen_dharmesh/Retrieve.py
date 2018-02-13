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
    writes = ['bemullen_dharmesh.cityscore', 'bemullen_dharmesh.universities']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        # url = 'http://datamechanics.io/data/bemullen_dharmesh/data/311ServiceCalls.json'
        urls = {'cityscore': 'https://data.boston.gov/datastore/odata3.0/5bce8e71-5192-48c0-ab13-8faff8fef4d7?$format=json',
        'universities':'https://data.boston.gov/datastore/odata3.0/90ed3816-5e70-443c-803d-9a71f44470be?$format=json',
        'code_enforcements': 'https://data.boston.gov/datastore/odata3.0/90ed3816-5e70-443c-803d-9a71f44470be?$format=json'}

        for (key, url) in urls.items():
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)['value']
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropCollection(key)
            repo.createCollection(key)
            repo['bemullen_dharmesh.' + key].insert_many(r) 

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("universities")
        repo.createCollection("universities")
        repo['bemullen_dharmesh.universities'].insert_many(r)
        # print(repo['bemullen_dharmesh.universities'].metadata())

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
        doc.add_namespace('bgis', 'https://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:bemullen_dharmesh#Retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # TODO: Refactor
        resource_cityscore = doc.entity('bdpm:5bce8e71-5192-48c0-ab13-8faff8fef4d7',
            {'prov:label':'CityScore', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cityscore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve City Score metrics'})
        doc.wasAssociatedWith(get_cityscore, this_script)
        doc.usage(get_cityscore, resource_cityscore, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$format=json'
                  })

        resource_universities = doc.entity('bgis:cbf14bb032ef4bd38e20429f71acb61a_2',
            {'prov:label':'Coordinates of Universities in Boston',
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_universities = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label': 'Retrieve coordinates of Universities in Boston'})
        doc.wasAssociatedWith(get_universities, this_script)
        doc.usage(get_universities, resource_universities, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        resource_code_enforcements = doc.entity('bdpm:0ed3816-5e70-443c-803d-9a71f44470be',
            {'prov:label':'Code Enforcement - Building and Property', prov.model.PROV_TYPE:'ont:DataResource',
            'ont:Extension':'json'})
        get_code_enforcements = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Get Code Enforcement Data'})
        doc.wasAssociatedWith(get_code_enforcements, this_script)
        doc.usage(get_code_enforcements, resource_code_enforcements, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?$format=json'
            })

        cityscore = doc.entity('dat:bemullen_dharmesh#cityscore', {prov.model.PROV_LABEL:'CityScore Metrics',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cityscore, this_script)
        doc.wasGeneratedBy(cityscore, get_cityscore, endTime)
        doc.wasDerivedFrom(cityscore, resource_cityscore, get_cityscore,
            get_cityscore, get_cityscore)

        universities = doc.entity('dat:bemullen_dharmesh#universities',
            {prov.model.PROV_LABEL:'Coordinates of Universities in Boston',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(universities, this_script)
        doc.wasGeneratedBy(universities, get_universities, endTime)
        doc.wasDerivedFrom(universities, resource_universities, get_universities,
            get_universities, get_universities)

        code_enforcements = doc.entity('dat:bemullen_dharmesh#code_enforcements',
            {prov.model.PROV_LABEL:'Code Enforcement - Building and Property',
            prov.model.PROV_TYPE:'ont:DataSet'
            })
        doc.wasAttributedTo(code_enforcements, this_script)
        doc.wasGeneratedBy(code_enforcements, get_code_enforcements, endTime)
        doc.wasDerivedFrom(code_enforcements, resource_code_enforcements, get_code_enforcements,
            get_code_enforcements, get_code_enforcements)

        repo.logout()
                  
        return doc

Retrieve.execute()
doc = Retrieve.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
