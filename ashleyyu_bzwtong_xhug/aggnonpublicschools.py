import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class aggnonpublicschools(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = ['ashleyyu_bzwtong.nonpublicschools']
    writes = ['ashleyyu_bzwtong.aggnonpublicschools']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        repo.dropPermanent("aggnonpublicschools")
        repo.createPermanent("aggnonpublicschools")
        

    
        nonpublicschools = list(repo.ashleyyu_bzwtong.nonpublicschools.find())

        zipCount= []
        for entry in nonpublicschools[0]["features"]:
            if "ZIP" in entry["properties"]:
                zipcd = entry["properties"]["ZIP"]
                print(zipcd)
                zipCount += [(zipcd, 1)]
        keys = {r[0] for r in zipCount}
        agg_val= [(key, sum([n for (z,n) in zipCount if z == key])) for key in keys]

        final= []
        for entry in agg_val:
            final.append({'nonpublicschoolsZipcode:':entry[0], 'nonpublicschoolsCount':entry[1]})
        print (final)
        repo['ashleyyu_bzwtong.aggnonpublicschools'].insert_many(final)
             
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


        this_script = doc.agent('alg:ashleyyu_bzwtong#aggnonpublicschools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:ashleyyu_bzwtong#nonpublicschools', {'prov:label':' Non Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggnonpublicschools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggnonpublicschools, this_script)
        doc.usage(get_aggnonpublicschools, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggnonpublicschools = doc.entity('dat:ashleyyu_bzwtong#aggnonpublicschools', {prov.model.PROV_LABEL:' Non Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggnonpublicschools, this_script)
        doc.wasGeneratedBy(aggnonpublicschools, get_aggnonpublicschools, endTime)
        doc.wasDerivedFrom(aggnonpublicschools, resource_properties, get_aggnonpublicschools, get_aggnonpublicschools, get_aggnonpublicschools)



        repo.logout()
                  
        return doc

aggnonpublicschools.execute()
doc = aggnonpublicschools.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
