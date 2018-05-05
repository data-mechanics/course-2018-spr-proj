import urllib.request
import dml
import prov.model
import datetime
import uuid

class fetchIncomeData(dml.Algorithm):
    contributor = 'jlove'
    reads = []
    writes = ['jlove.incomes']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        
        repo.dropCollection("incomes")
        repo.createCollection("incomes")
                
        data = None
        url = 'http://datamechanics.io/data/jlove/neighborhood_incomes.csv'
        response = urllib.request.urlopen(url)
        if response.status == 200:
            data = response.read().decode('utf-8')
        if data != None:
            lines = data.splitlines()
            categories = lines[0].split(',')
            entries = []
            for line in lines[1:]:
                columns = line.split(',')
                entry = {}
                for i in range(len(categories)):
                    try:
                        entry[categories[i]] = int(columns[i])
                    except ValueError:
                        entry[categories[i]] = columns[i]
                entries += [entry]
            repo['jlove.incomes'].insert_many(entries)
            repo['jlove.incomes'].metadata({'complete':True})
            print(repo['jlove.incomes'].metadata())
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
        
        
        this_script = doc.agent('alg:jlove#fetchIncomeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:jlove/neighborhood_incomes', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        fetch_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(fetch_income, this_script)
        doc.usage(fetch_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        incomes = doc.entity('dat:jlove#incomes', {prov.model.PROV_LABEL:'Boston Median Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(incomes, this_script)
        doc.wasGeneratedBy(incomes, fetch_income, endTime)
        doc.wasDerivedFrom(incomes, resource, fetch_income, fetch_income, fetch_income)


        repo.logout()
                  
        return doc