import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv


class income(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = []
    writes = ['yuxiao_yzhang11.income']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')
        url = 'https://data.cambridgema.gov/resource/ixt9-srjw.json'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        raw = json.loads(response)
        s = json.dumps(raw, sort_keys=True, indent=2)
        
        repo.dropCollection("medianIncome")
        repo.createCollection("medianIncome")

        repo['yuxiao_yzhang11.medianIncome'].insert_many(raw)
        repo['yuxiao_yzhang11.medianIncome'].metadata({'complete':True})
        print(repo['yuxiao_yzhang11.medianIncome'].metadata())

       

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
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cambridgema.gov/resource')

        this_script = doc.agent('alg:yuxiao_yzhang11#income', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:ixt9-srjw', {'prov:label':'median, income', prov.model.PROV_TYPE:'ont:rows', 'ont:Extension':'json'})
        
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        
        ## need to know what is this
        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        output = doc.entity('dat:yuxiao_yzhang11.medianIncome', {prov.model.PROV_LABEL:'medianIncome', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        # found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(found, this_script)
        # doc.wasGeneratedBy(found, get_found, endTime)
        # doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

income.execute()
doc = income.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
