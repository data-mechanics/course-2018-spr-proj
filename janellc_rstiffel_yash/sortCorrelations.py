import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
"""
Finds average point (lat, long) for each street in each district where crimes existed.
This is for finding the "middle" of the street - used in findCrimeStats.

Yields the form {DISTRICT: {"street1": {Lat: #, Long: #}, "street2": {Lat: #, Long: #} ...}, DISTRICT2:...}

- Filters out empty entries

"""




class sortCorrelations(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = ['janellc_rstiffel_yash.sortedNeighborhoods']
    writes = ['janellc_rstiffel_yash.coorelations']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        # Get Crime data and neighborhood data
        # street light data borrowed from ferrys

        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        df = list(repo.janellc_rstiffel_yash.sortedNeighborhoods.find())
        items = []
        for item in df:
        	items.append(item.get('data'))
        items = pd.DataFrame(items)
        # @staticmethod

        corr = pd.DataFrame(items.corr())

        repo.dropCollection("coorelations")
        repo.createCollection("coorelations")

        r = {'field1': 'crimes', 'field2': 'streetlights', 'value': corr['lights']['crimes']}
        repo['janellc_rstiffel_yash.coorelations'].insert(r)
        repo['janellc_rstiffel_yash.coorelations'].metadata({'complete':True})
        print(repo['janellc_rstiffel_yash.coorelations'].metadata())


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
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        # Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel_yash#sortCorrelations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Resource = crimesData
        resource1 = doc.entity('dat:ferrys#streetlights', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # Resource = crimesData
        resource2 = doc.entity('dat:janellc_rstiffel_yash#sortedNeighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        #Activity
        find_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(find_correlation, this_script)

        doc.usage(find_correlation, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(find_correlation, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        corr = doc.entity('dat:janellc_rstiffel_yash#coorelation', {prov.model.PROV_LABEL:'Correlation between streetlights and crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr, this_script)
        doc.wasGeneratedBy(corr, find_correlation, endTime)
        doc.wasDerivedFrom(corr, resource1, resource2, find_correlation, find_correlation)

        repo.logout()
                  
        return doc

#sortCorrelations.execute()
#doc = sortCorrelations.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
