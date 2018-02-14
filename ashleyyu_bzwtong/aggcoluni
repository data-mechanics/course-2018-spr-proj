import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class aggcoluni(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = ['ashleyyu_bzwtong.collegeanduni']
    writes = ['ashleyyu_bzwtong.aggcoluni']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        repo.dropPermanent("aggcoluni")
        repo.createPermanent("aggcoluni")

        zipCount= []
        for entry in repo.ashleyyu_bzwtong.collegeanduni.find():
            if "zipcode" in entry:
                zipcd = entry["zipcode"]
                zipCount += [(zipcd, 1)]
                
                
        keys = {k[0] for k in zipCount}
        agg_val= [(key, sum([n for (z,n) in zipCount if z == key])) for key in keys]

        final= []
        for entry in agg_val:
            final.append({'coluniZipcode:':entry[0], 'coluniCount':entry[1]})

        repo['ashleyyu_bzwtong.aggcoluni'].insert_many(final)
        
        for entry in repo.ashleyyu_bzwtong.aggcoluni.find():
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


        this_script = doc.agent('alg:ashleyyu_bzwtong#aggcoluni', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:ashleyyu_bzwtong#collegeanduni', {'prov:label':' College and Uni Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggcoluni = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggcoluni, this_script)
        doc.usage(get_aggcoluni, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggcoluni = doc.entity('dat:ashleyyu_bzwtong#aggcoluni', {prov.model.PROV_LABEL:' College and Uni Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggcoluni, this_script)
        doc.wasGeneratedBy(aggcoluni, get_aggcoluni, endTime)
        doc.wasDerivedFrom(aggcoluni, resource_properties, get_aggcoluni, get_aggcoluni, get_aggcoluni)



        repo.logout()
                  
        return doc

aggcoluni.execute()
doc = aggcoluni.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
