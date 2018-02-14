import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

import pymongo
from bson.code import Code
import pprint


class sf_evictions_constructions(dml.Algorithm):

    contributor = 'agoncharova_lmckone'
    reads = ['agoncharova_lmckone.sf_evictions', 'agoncharova_lmckone.sf_permits']
    writes = ['agoncharova_lmckone.sf_evictions_constructions']

    def aggregate(R,f):
    	'''helper methods courtsey of Profesoor Andrei Lapets'''
    	keys = {r[0] for r in R}
    	return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Approved Building Permits dataset'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
        repo.dropCollection("sf_evictions_constructions")
        repo.createCollection("sf_evictions_constructions")

        sf_evictions = repo['agoncharova_lmckone.sf_evictions']
        sf_permits = repo['agoncharova_lmckone.sf_permits']


        evictions_zip = []

        for eviction in sf_evictions.find():
        	if 'zip' in eviction:
        		zipcode = eviction['zip']
        		evictions_zip.append((zipcode, 1))

        #aggregate count of evictions by summing the ones
        evictions_by_zip = sf_evictions_constructions.aggregate(evictions_zip, sum)

        constructions_zip = []
        #filter constructions for new construction permit type
        for construction in sf_permits.find({'permit_type': '1'}):
        	if 'zipcode' in construction:
        		zipcode = construction['zipcode']
        		constructions_zip.append((zipcode, 1))

        #aggregate count of constructions by summing the ones
        constructions_by_zip = sf_evictions_constructions.aggregate(constructions_zip, sum)

        #join evictions and constructions by ZIP code
        evictions_constructions = sf_evictions_constructions.project(sf_evictions_constructions.select(sf_evictions_constructions.product(evictions_by_zip,constructions_by_zip), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        #format data to save in db
        data_to_save = []
        for record in evictions_constructions:
            data_to_save.append({'zip': record[0], 'evictions': record[1],'permits': record[2] })

        #insert data into repo
        repo['agoncharova_lmckone.sf_evictions_constructions'].insert_many(data_to_save)
        repo['agoncharova_lmckone.sf_evictions_constructions'].metadata({'complete':True})
        #print(repo['agoncharova_lmckone.boston_permits'].metadata())

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

        this_script = doc.agent('alg:agoncharova_lmckone#sf_evictions_constructions', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_evictions = doc.entity('dat:agoncharova_lmckone#sf_evictions', {'prov:label':'San Francisco Evictions', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_permits = doc.entity('dat:agoncharova_lmckone#sf_permits', {'prov:label':'San Francisco Permits', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_sf_evictions_constructions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_sf_evictions_constructions, this_script)

        doc.usage(get_sf_evictions_constructions, resource_evictions, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        doc.usage(get_sf_evictions_constructions, resource_permits, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        sf_evictions_constructions = doc.entity('dat:agoncharova_lmckone#sf_evictions_constructions', {prov.model.PROV_LABEL: 'Count of evictions and new constructions in SF by ZIP Code', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(sf_evictions_constructions, this_script)
        doc.wasGeneratedBy(sf_evictions_constructions, get_sf_evictions_constructions, endTime)
        doc.wasDerivedFrom(sf_evictions_constructions, resource_evictions, get_sf_evictions_constructions, get_sf_evictions_constructions, get_sf_evictions_constructions)
        doc.wasDerivedFrom(sf_evictions_constructions, resource_permits, get_sf_evictions_constructions, get_sf_evictions_constructions, get_sf_evictions_constructions)
        
        repo.logout()
                  
        return doc


#sf_evictions_constructions.execute()
