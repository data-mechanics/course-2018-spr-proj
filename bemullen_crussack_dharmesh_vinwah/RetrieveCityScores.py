# Filename: RetrieveScores.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Retrieve datasets from the sources provided and generate 
#              the data lineage.
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveCityScores(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.cityscores"]


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

        key = "cityscores"
        url = RetrieveCityScores.parseURL('''https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "5bce8e71-5192-48c0-ab13-8faff8fef4d7" WHERE "ETL_LOAD_DATE" >= '2016-02-01 00:00:00' AND "ETL_LOAD_DATE" <= '2018-01-01 00:00:00' ''')

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
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveCityScores',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_cityscores = doc.entity('bdpm:5bce8e71-5192-48c0-ab13-8faff8fef4d7',
            {'prov:label':'CityScores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cityscores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve City Score metrics for Boston City'})
        doc.wasAssociatedWith(get_cityscores, this_script)
        doc.usage(get_cityscores, resource_cityscores, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'''?sql=SELECT * from "5bce8e71-5192-48c0-ab13-8faff8fef4d7" WHERE "ETL_LOAD_DATE" >= '2016-02-01 00:00:00' AND "ETL_LOAD_DATE" <= '2018-01-01 00:00:00' '''
                  })

        cityscores = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#cityscores', {prov.model.PROV_LABEL:'CityScore Metrics',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cityscores, this_script)
        doc.wasGeneratedBy(cityscores, get_cityscores, endTime)
        doc.wasDerivedFrom(cityscores, resource_cityscores, get_cityscores,
            get_cityscores, get_cityscores)

        repo.logout()
                  
        return doc
RetrieveCityScores.execute()
doc = RetrieveCityScores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
