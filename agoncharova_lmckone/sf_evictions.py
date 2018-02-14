import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

class sf_evictions(dml.Algorithm):
    contributor = 'agoncharova_lmckone'
    reads = []
    writes = ['agoncharova_lmckone.sf_evictions']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Assessing Data from 2014-2017'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        #2014
        repo.dropCollection("sf_evictions")
        repo.createCollection("sf_evictions")

        url = 'https://data.sfgov.org/resource/93gi-sfd2.json'  
        response = ur.urlopen(url)
        data = json.load(response)

        repo['agoncharova_lmckone.sf_evictions'].insert_many(data)
        repo['agoncharova_lmckone.sf_evictions'].metadata({'complete':True})

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
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('sfdp', 'https://datasf.org/opendata/')

        this_script = doc.agent('alg:agoncharova_lmckone#sf_evictions', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('sfdp:93gi-sfd2', {'prov:label':'San Francisco Data Portal Evictions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_sf_evictions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sf_evictions, this_script)

        doc.usage(get_sf_evictions, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        sf_evictions = doc.entity('dat:agoncharova_lmckone#sf_evictions', {prov.model.PROV_LABEL:'San Francisco Evictions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sf_evictions, this_script)
        doc.wasGeneratedBy(sf_evictions, get_sf_evictions, endTime)
        doc.wasDerivedFrom(sf_evictions, resource, get_sf_evictions, get_sf_evictions, get_sf_evictions)
        repo.logout()
                  
        return doc
