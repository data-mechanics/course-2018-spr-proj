import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint

class mergeAreaIncome(dml.Algorithm):
    contributor = 'ybavishi'
    reads = ['yash.rentData','yash.householdIncomeData']
    writes = ['yash.areaIncomesData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yash', 'yash')
        areas = repo['yash.rentData']
        incomes = repo['yash.householdIncomeData']
        
        area_records = []
        for area in areas.find():
            area_dict = {}
            area_dict['Region'] = area['city']
            income = incomes.find_one({"Region":area['city']})
            if income:
                area_dict['Income'] = income['Median Income']
            else:
                area_dict['Income'] = 'N/A'
            area_records.append(area_dict.copy())


        repo.dropCollection("yash.areaIncomesData")
        repo.createCollection("yash.areaIncomesData")
        repo['yash.areaIncomesData'].insert_many(area_records)
        repo['yash.areaIncomesData'].metadata({'complete':True})
        print(repo['yash.areaIncomesData'].metadata())


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

        this_script = doc.agent('alg: mergeAreaIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:rentData', {'prov:label':'Rent Areas', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:householdIncomeData', {'prov:label':'Household Income', prov.model.PROV_TYPE:'ont:DataResource'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_prices, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )


        prices = doc.entity('dat:areaIncomesData', {prov.model.PROV_LABEL:'Household Incomes in Rent Areas', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)
        doc.wasDerivedFrom(prices, resource2, get_prices, get_prices, get_prices)


      
        repo.logout()
                  
        return doc

mergeAreaIncome.execute()
doc = mergeAreaIncome.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

#eof