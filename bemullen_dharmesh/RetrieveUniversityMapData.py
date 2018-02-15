import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveUniversityMapData(dml.Algorithm):
    contributor = "bemullen_dharmesh"
    reads = []
    writes = ["bemullen_dharmesh.universities"]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        repo.dropCollection("universities")
        repo.createCollection("universities")
        repo['bemullen_dharmesh.universities'].insert_many(r)

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
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('bgis', 'https://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_dharmesh/data/')
        doc.add_namespace('bgis', 'https://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:bemullen_dharmesh#Retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_universities = doc.entity('bgis:cbf14bb032ef4bd38e20429f71acb61a_2',
            {'prov:label':'Coordinates of Universities in Boston',
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_universities = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label': 'Retrieve coordinates of Universities in Boston'})
        doc.wasAssociatedWith(get_universities, this_script)
        doc.usage(get_universities, resource_universities, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        universities = doc.entity('dat:bemullen_dharmesh#universities',
            {prov.model.PROV_LABEL:'Coordinates of Universities in Boston',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(universities, this_script)
        doc.wasGeneratedBy(universities, get_universities, endTime)
        doc.wasDerivedFrom(universities, resource_universities, get_universities,
            get_universities, get_universities)

        repo.logout()
        return doc
