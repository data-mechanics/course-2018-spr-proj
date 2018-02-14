
import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

class sf_permits(dml.Algorithm):
    contributor = 'agoncharova_lmckone'
    reads = []
    writes = ['agoncharova_lmckone.sf_permits']

    @staticmethod
    def execute(trial = False):
        '''Retrieve SF Permit Data'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        #2014
        repo.dropCollection("sf_permits")
        repo.createCollection("sf_permits")

        url = 'https://data.sfgov.org/resource/vy2q-29it.json'  
        response = ur.urlopen(url)
        data = json.load(response)

        repo['agoncharova_lmckone.sf_permits'].insert_many(data)
        repo['agoncharova_lmckone.sf_permits'].metadata({'complete':True})

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

        this_script = doc.agent('alg:agoncharova_lmckone#sf_permits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('sfdp:vy2q-29it', {'prov:label':'San Francisco Data Portal Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_sf_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sf_permits, this_script)

        doc.usage(get_sf_permits, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        sf_permits = doc.entity('dat:agoncharova_lmckone#sf_permits', {prov.model.PROV_LABEL:'San Francisco Permits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sf_permits, this_script)
        doc.wasGeneratedBy(sf_permits, get_sf_permits, endTime)
        doc.wasDerivedFrom(sf_permits, resource, get_sf_permits, get_sf_permits, get_sf_permits)
        repo.logout()
                  
        return doc
