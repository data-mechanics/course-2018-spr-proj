import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson


class getPovertyRatesData(dml.Algorithm):
    contributor = 'ybavishi'
    reads = []
    writes = ['yash.povertyRatesData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yash', 'yash')

        url = 'http://datamechanics.io/data/ybavishi/poverty_rates.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("povertyRatesData")
        repo.createCollection("povertyRatesData")
        repo['yash.povertyRatesData'].insert_many(r)
        repo['yash.povertyRatesData'].metadata({'complete':True})
        print(repo['yash.povertyRatesData'].metadata())

       
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
        repo.authenticate('yash', 'yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ybavishi#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ybavishi#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dm', 'http://datamechanics.io/data/ybavishi/')

        this_script = doc.agent('alg:getPovertyRatesData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dm:poverty_rates', {'prov:label':'Poverty Rates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)
        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        
        prices = doc.entity('dat:povertyRatesData', {prov.model.PROV_LABEL:'Poverty Rates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)


        repo.logout()
                  
        return doc

getPovertyRatesData.execute()
doc = getPovertyRatesData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof