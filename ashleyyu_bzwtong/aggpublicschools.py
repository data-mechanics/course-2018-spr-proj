import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class aggpublicschools(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = ['ashleyyu_bzwtong.publicschools']
    writes = ['ashleyyu_bzwtong.aggpublicschools']
    
    
    @staticmethod
    def execute(trial = False):
        '''Find number of public schools within each zipcode'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        repo.dropPermanent("aggpublicschools")
        repo.createPermanent("aggpublicschools")
        
        publicschools = list(repo.ashleyyu_bzwtong.publicschools.find())

        zipCount= []
        for entry in publicschools:
            if "zipcode" in entry:
                zipcd = entry["zipcode"]
                zipCount += [(zipcd, 1)]
                    
        keys = {r[0] for r in zipCount}
        agg_val= [(key, sum([n for (z,n) in zipCount if z == key])) for key in keys]

        final= []
        for entry in agg_val:
            final.append({'publicschoolsZipcode:':entry[0], 'publicschoolsCount':entry[1]})

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')


        this_script = doc.agent('alg:ashleyyu_bzwtong#publicschoolsAgg', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_properties = doc.entity('dat:ashleyyu_bzwtong#schools', {'prov:label':' Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_publicschoolsAgg = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggpublicschools, this_script)
        doc.usage(get_aggpublicschools, resource_properties, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        publicschoolsAgg = doc.entity('dat:ashleyyu_bzwtong#publicschoolsAgg', {prov.model.PROV_LABEL:' Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(publicschoolsAgg, this_script)
        doc.wasGeneratedBy(publicschoolsAgg, get_publicschoolsAgg, endTime)
        doc.wasDerivedFrom(publicschoolsAgg, resource_properties, get_publicschoolsAgg, get_publicschoolsAgg, get_publicschoolsAgg)



        repo.logout()
                  
        return doc

#aggpublicschools.execute()
#doc = aggpublicschools.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
