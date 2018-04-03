import dml
import prov.model
import datetime
import uuid
import shapely.geometry
import numpy

class normalizeIncomeData(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.income', 'jlove.neighborhoods', 'jlove.incomeNormalized']
    writes = ['jlove.incomeNormalized', 'jlove.nhWithIncome']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        incomes = repo['jlove.incomes'].find({})
        incomeList = []
        results = []
        for income in incomes:
            results += [income]
            incomeList += [income["Median Household Income"]]
        
        mean = numpy.mean(incomeList)
        stdv = numpy.std(incomeList)
        
        
        for elem in results:
            elem['normalized'] = (elem["Median Household Income"] - mean)/stdv
        
        repo.dropCollection('incomeNormalized')
        repo.createCollection('incomeNormalized')
        repo['jlove.incomeNormalized'].insert_many(results)
        
        neighborhoods = repo['jlove.neighborhoods'].find_one({})
        incomes = repo['jlove.incomeNormalized'].find({})
        
        features = neighborhoods['features']
        for income in incomes:
            for feature in features:
                if feature['properties']['Name'] == income['Neighborhood']:
                    feature['properties']['medianIncome'] = income['Median Household Income']
                    feature['properties']['normalizedIncome'] = income['normalized']
        
        repo.dropCollection('nhWithIncome')
        repo.createCollection('nhWithIncome')
        
        repo['jlove.nhWithIncome'].insert_one(neighborhoods)
            
        repo.logout()
        
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
        
        
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/dataset/')
        
        
        this_script = doc.agent('alg:jlove#normalizeIncomeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('dat:jlove#income', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        normalize_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource2 = doc.entity('dat:jlove#neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource3 = doc.entity('dat:jlove#incomeNormalized', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        income_normalized = doc.entity('dat:jlove#incomeNormalized', {prov.model.PROV_LABEL:'Normalized Boston Median Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income_normalized, this_script)
        doc.wasGeneratedBy(income_normalized, normalize_income, endTime)
        doc.wasDerivedFrom(income_normalized, resource1, normalize_income, normalize_income, normalize_income)
        
        nh_income = doc.entity('dat:jlove#nhWithIncome', {prov.model.PROV_LABEL:'Boston Neighborhood Borders with Information About Median Household Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(nh_income, this_script)
        doc.wasGeneratedBy(nh_income, normalize_income, endTime)
        doc.wasDerivedFrom(nh_income, resource3, normalize_income, normalize_income, normalize_income)
        doc.wasDerivedFrom(nh_income, resource2, normalize_income, normalize_income, normalize_income)


        repo.logout()
                  
        return doc