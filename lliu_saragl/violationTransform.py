import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid
from collections import Counter

def aggregate(R,f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

class violationTransform(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = ['lliu_saragl.violations']
    writes = ['lliu_saragl.violationTransform']


    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        repo.dropPermanent("violationTransform")
        repo.createPermanent("violationTransform")

        def getData(db):
            d = []
            for i in repo['lliu_saragl.' + db].find():
                d.append(i)
            return d

        v = getData('violations')

        lstOfViolations = {}
        for i in v:
            temp = i['Zip']
            if 'Zip' in i:
                lstOfViolations[i['Zip']] = lstOfViolations.get(i['Zip'], 0) + 1
            
        print(lstOfViolations)
       
        repo['lliu_saragl.violationTransform'].insert_one(lstOfViolations)

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenacne document describing everything happening in this script.
        Each run of the script will generate a new document describing that invocation event.
        '''

        # Set up the database connection 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:lliu_saragl#violationTransform', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],'ont:Extension':'py'})
        resource = doc.entity('dat:lliu_saragl#violations',{prov.model.PROV_LABEL:'Violations', prov.model.PROV_TYPE:'ont:DataSet'})

        get_violation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computations'})
        doc.wasAttributedTo(resource, this_script)
        doc.wasGeneratedBy(resource, get_violation, endTime)
        doc.wasDerivedFrom(resource, get_violation, get_violation, get_violation)

        repo.logout()
        return doc

#violationTransform.execute()
#doc = violationTransform.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

        
