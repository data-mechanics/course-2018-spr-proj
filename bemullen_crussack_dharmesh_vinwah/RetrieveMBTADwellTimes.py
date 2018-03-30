# File: RetrieveMBTADwellTimes.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class RetrieveMBTADwellTimes(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.mbta_red_dwells", "bemullen_crussack_dharmesh_vinwah.mbta_green_dwells"]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        urls = {'mbta_red_dwells': 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/mbta_red_dwells.json',
        'mbta_green_dwells': 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/mbta_green_dwells.json'}

        for (key, url) in urls.items():
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)[key]
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
        doc.add_namespace('bgis', 'https://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveMBTADwellTimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_mbta_red_dwells = doc.entity('datp:mbta_red_dwells',
            {'prov:label':'MBTA Red Line Dwell Values', prov.model.PROV_TYPE:'ont:DataResource',
            'ont:Extension': 'json'})
        get_mbta_red_dwells = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label': 'Get List of Time Spent waiting at each station (Red line)'})
        doc.wasAssociatedWith(get_mbta_red_dwells, this_script)
        doc.usage(get_mbta_red_dwells, resource_mbta_red_dwells, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        resource_mbta_green_dwells = doc.entity('datp:mbta_green_dwells',
            {'prov:label':'MBTA Green Line Dwell Values', prov.model.PROV_TYPE:'ont:DataResource',
            'ont:Extension': 'json'})
        get_mbta_green_dwells = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label': 'Get List of Time Spent waiting at each station (Green line)'})
        doc.wasAssociatedWith(get_mbta_green_dwells, this_script)
        doc.usage(get_mbta_green_dwells, resource_mbta_green_dwells, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})
        

        mbta_red_dwells = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#mbta_red_dwells',
            {prov.model.PROV_LABEL:'MBTA Red Line Dwell Intervals',
            prov.model.PROV_TYPE:'ont:DataSet'
            })
        doc.wasAttributedTo(mbta_red_dwells, this_script)
        doc.wasGeneratedBy(mbta_red_dwells, get_mbta_red_dwells, endTime)
        doc.wasDerivedFrom(mbta_red_dwells, resource_mbta_red_dwells, get_mbta_red_dwells,
            get_mbta_red_dwells, get_mbta_red_dwells)

        mbta_green_dwells = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#mbta_green_dwells',
            {prov.model.PROV_LABEL:'MBTA Green Line Dwell Intervals',
            prov.model.PROV_TYPE:'ont:DataSet'
            })
        doc.wasAttributedTo(mbta_green_dwells, this_script)
        doc.wasGeneratedBy(mbta_green_dwells, get_mbta_green_dwells, endTime)
        doc.wasDerivedFrom(mbta_green_dwells, resource_mbta_green_dwells, get_mbta_green_dwells,
            get_mbta_green_dwells, get_mbta_green_dwells)

        repo.logout()
                  
        return doc

if __name__ == "__main__":
    RetrieveMBTADwellTimes.execute()