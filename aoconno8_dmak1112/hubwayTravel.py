import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

class hubwayTravel(dml.Algorithm):
    contributor = 'aoconno8_dmak1112'
    reads = []
    writes = ['aoconno8_dmak1112.hubwayTravel']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')

        url_one = 'http://datamechanics.io/data/aoconno8_dmak1112/hubway_travel_Jan-Oct.csv'
        df = pd.read_csv(url_one)

        url_two = 'http://datamechanics.io/data/aoconno8_dmak1112/hubway_travel_Nov-Dec.csv'
        df2 = pd.read_csv(url_two)  

        df.append(df2)

        new_df = df.filter(['tripduration', 'starttime', 'stoptime', 'start station id', \
                   'end station id', 'birth year', 'gender'], axis=1)

        final_data = new_df.to_dict(orient='records')
        
        repo.dropCollection("hubwayTravel")
        repo.createCollection("hubwayTravel")
        repo['aoconno8_dmak1112.hubwayTravel'].insert_many(final_data)
        repo['aoconno8_dmak1112.hubwayTravel'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112.hubwayTravel'].metadata())

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
        repo.authenticate('aoconno8_dmak1112', 'aoconno8_dmak1112')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aoconno8_dmak1112') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aoconno8_dmak1112') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:aoconno8_dmak1112#hubwayTravel', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        jan_oct = doc.entity('dat:hubway_travel_Jan-Oct', {'prov:label':'Hubway Travel Data January-October 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        nov_dec = doc.entity('dat:hubway_travel_Nov-Dec', {'prov:label':'Hubway Travel Data November-December 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_hubwayTravel = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubwayTravel, this_script)
        doc.usage(get_hubwayTravel, jan_oct, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_hubwayTravel, nov_dec, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        hubwayTravel = doc.entity('dat:aoconno8_dmak1112#hubwayTravel', {prov.model.PROV_LABEL:'Hubway Daily Travel Data 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayTravel, this_script)
        doc.wasGeneratedBy(hubwayTravel, get_hubwayTravel, endTime)
        doc.wasDerivedFrom(hubwayTravel, jan_oct, get_hubwayTravel, get_hubwayTravel, get_hubwayTravel)
        doc.wasDerivedFrom(hubwayTravel, nov_dec, get_hubwayTravel, get_hubwayTravel, get_hubwayTravel)

        repo.logout()
                  
        return doc

hubwayTravel.execute()
doc = hubwayTravel.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
