import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#import numpy as np

class aggpublicschools(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = ['ashleyyu_bzwtong.publicschools']
    writes = ['ashleyyu_bzwtong.aggpublicschools']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        repo.dropPermanent("aggpublicschools")
        repo.createPermanent("aggpublicschools")

        zipCount= []
        for entry in repo.ashleyyu_bzwtong.publicschools.find():
            if "zipcode" in entry:
                zip = entry["zipcode"]
                zipCount += [(zip, 1)]
                
                
        keys = {k[0] for k in zipCount}
        agg_val= [(key, sum([n for (z,n) in zipCount if z == key])) for key in keys]

        final= []
        for entry in agg_val:
            final.append({'publicschoolsZipcode:':entry[0], 'schoolsCount':entry[1]})

        repo['ashleyyu_bzwtong.aggpublicschools'].insert_many(final)
        
        for entry in repo.ashleyyu_bzwtong.aggpublicschools.find():
             print(entry)
             
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
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:ashleyyu_bzwtong#aggpublicschools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:ashleyyu_bzwtong#publicschools', {'prov:label':' Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggpublicschools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggpublicschools, this_script)
        doc.usage(get_aggpublicschools, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggpublicschools = doc.entity('dat:ashleyyu_bzwtong#aggpublicschools', {prov.model.PROV_LABEL:' Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggpublicschools, this_script)
        doc.wasGeneratedBy(aggpublicschools, get_aggpublicschools, endTime)
        doc.wasDerivedFrom(aggpublicschools, resource_properties, get_aggpublicschools, get_aggpublicschools, get_aggpublicschools)



        repo.logout()
                  
        return doc
