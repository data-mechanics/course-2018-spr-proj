# Filename: Retrieve311Requests.py
# Authors: Dharmesh Tarapore <dharmesh@bu.edu>,
#         Vincent Wahl <vinwah@bu.edu>
# Description: Retrieves and transforms 311 service
#              requests to solve an optimization problem.
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import requests

class Retrieve311Requests(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.service_requests"]

    @staticmethod
    def parseURL(url):
        return quote(url, safe='://*\'?=')

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        key = "service_requests"
        # Retrive 311 data from data.boston.gov from period 2016-02-01 to 2018-03-30
        url = "https://cs-people.bu.edu/dharmesh/cs591/591data/service_requests_filtered.json"
        response = requests.get(url).text

        r = json.loads(response)

        repo.dropCollection(key)
        repo.createCollection(key)
        repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many(r)

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
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#Retrieve311Requests',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_311requests = doc.entity('csdt:service_requests_filtered',
            {'prov:label':'311Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_311requests = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve 311 Service Requests from period 2016-02-01 to 2018-03-30\
            for Boston City'})
        doc.wasAssociatedWith(get_311requests, this_script)
        doc.usage(get_311requests, resource_311requests, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        service_requests = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#service_requests',\
            {prov.model.PROV_LABEL:'311 Service Requests for Boston City',
            prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(service_requests, this_script)
        doc.wasGeneratedBy(service_requests, get_311requests, endTime)
        doc.wasDerivedFrom(service_requests, resource_311requests, get_311requests,
            get_311requests, get_311requests)

        repo.logout()
                  
        return doc