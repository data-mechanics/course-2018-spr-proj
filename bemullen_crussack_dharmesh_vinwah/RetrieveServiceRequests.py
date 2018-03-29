# Filename: RetrieveServiceRequests.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveServiceRequests(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ['bemullen_crussack_dharmesh_vinwah.service_requests']

    @staticmethod
    def parseURL(url):
        return quote(url, safe='://*\'?=')

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        key = "service_requests"
        url = RetrieveServiceRequests.parseURL('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "2968e2c0-d479-49ba-a884-4ef523ada3c0" WHERE "open_dt" >= '2016-02-01 00:00:00' AND "open_dt" <= '2018-01-01 00:00:00' ''')

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['result']['records']
        repo.dropCollection(key)
        repo.createCollection(key)
        repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many(r)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveServiceRequests',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_service_requests = doc.entity('bdpm:2968e2c0-d479-49ba-a884-4ef523ada3c0',
            {'prov:label':'311 Service Requests', prov.model.PROV_TYPE:'ont:DataResource',
            'ont:Extension': 'json'})
        get_service_requests = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label': 'Get List of Boston\'s 311 Service Requests'})
        doc.wasAssociatedWith(get_service_requests, this_script)
        doc.usage(get_service_requests, resource_service_requests, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':''' ?sql=SELECT * from "2968e2c0-d479-49ba-a884-4ef523ada3c0" WHERE "open_dt" >= '2016-02-01 00:00:00' AND "open_dt" <= '2018-01-01 00:00:00' '''})

        service_requests = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#service_requests',
            {prov.model.PROV_LABEL:'311 Service Requests',
            prov.model.PROV_TYPE:'ont:DataSet'
            })
        doc.wasAttributedTo(service_requests, this_script)
        doc.wasGeneratedBy(service_requests, get_service_requests, endTime)
        doc.wasDerivedFrom(service_requests, resource_service_requests, get_service_requests,
            get_service_requests, get_service_requests)

        repo.logout()

        return doc

