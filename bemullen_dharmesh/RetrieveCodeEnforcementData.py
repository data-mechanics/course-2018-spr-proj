# Filename: RetrieveCodeEnforcementData.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveCodeEnforcementData(dml.Algorithm):
    contributor = "bemullen_dharmesh"
    reads = []
    writes = ['bemullen_dharmesh.code_enforcements']

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
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        key = "code_enforcements"
        url = RetrieveCodeEnforcementData.parseURL('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "90ed3816-5e70-443c-803d-9a71f44470be" WHERE "Status_DTTM" >= '2016-02-01 00:00:00' AND "Status_DTTM" <= '2018-01-01 00:00:00' LIMIT 5''')

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['result']['records']
        repo.dropCollection(key)
        repo.createCollection(key)
        repo['bemullen_dharmesh.' + key].insert_many(r)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')

        this_script = doc.agent('alg:bemullen_dharmesh#RetrieveCodeEnforcementData',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_code_enforcements = doc.entity('bdpr:90ed3816-5e70-443c-803d-9a71f44470be',
            {'prov:label':'Code Enforcement - Building and Property', prov.model.PROV_TYPE:'ont:DataResource',
            'ont:Extension':'json'})
        get_code_enforcements = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Get Property and Code Enforcement Data'})
        doc.wasAssociatedWith(get_code_enforcements, this_script)
        doc.usage(get_code_enforcements, resource_code_enforcements, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'''sql=SELECT * from "90ed3816-5e70-443c-803d-9a71f44470be" WHERE "Status_DTTM" >= '2016-02-01 00:00:00' AND "Status_DTTM" <= '2018-01-01 00:00:00' '''
            })

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

