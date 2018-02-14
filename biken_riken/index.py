import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class index(dml.Algorithm):
    contributor = 'biken_riken'
    reads = []
    writes = ['biken_riken.trail-index']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('biken_riken', 'biken_riken')
        
        # Dataset01
        url = "http://datamechanics.io/data/bm181354_rikenm/htaindex_data_places_25.csv"
        
        Boston_df = pd.read_csv(url)
        
        # Transportation cost for regional typical annually
        arr_transportation = Boston_df['t_cost_ami']
        # typical monthly housing cost
        arr_housing = Boston_df['h_cost']
        # neighborhood index
        arr_neighbor = Boston_df['compact_ndx']
        # name of the city of Greater Boston (key)
        city = Boston_df['name']


        # average across all over boston, this will be used for ratio among city as a scale
        average_transportation_cost = arr_transportation.mean()
        # applies value/average to all the row of data
        index_transport = arr_transportation/average_transportation_cost

        # Benchmark and ideal housing cost
        average_housing = arr_housing.mean()
        # applies value/average to all the row of data
        index_house = arr_housing/average_housing

        # combined all the computed data
        new_df = pd.DataFrame(
                 {'City': city,
                 'Housing': arr_housing,
                 'Housing_index':index_house ,
                 'Neighbor_index': arr_neighbor,
                 'Transportation_index':index_transport
                 
                 })

        r = json.loads(new_df.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)
        

        jup_repo = client.repo

        # clear
        repo.dropPermanent('trail-index')
        #repo.create_collection("trail_index")
        repo.createPermanent('trail-index')
        repo['trail_index'].insert_many(r)
        
        # logout
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
        
        repo.authenticate('biken_riken', 'biken_riken')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/data/bm181354_rikenm/')
        
        this_script = doc.agent('alg:biken_riken#liquorCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:htaindex_data_places_25', {'prov:label':'dataset of all liquor license in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        resource_two = doc.entity('bdp:crime', {'prov:label':'dataset of all criminal record in Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        get_liquor_license = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        get_crime_liquor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_liquor_license, this_script)
        
        #
        doc.usage(get_crime, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})
        doc.usage(get_liquor_license, resource_two, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                            })
                  
        liquor_license = doc.entity('dat:biken_riken#liquor-licenses', {prov.model.PROV_LABEL:'liquor license', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(liquor_license, this_script)
        doc.wasGeneratedBy(liquor_license, get_liquor_license, endTime)
        doc.wasDerivedFrom(liquor_license, resource, get_liquor_license, get_liquor_license, get_liquor_license)
                  
        crime = doc.entity('dat:biken_riken#crime-record', {prov.model.PROV_LABEL:'record of all crimes in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
                  
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_two,  get_crime,  get_crime, get_crime)
                  
        # one more for concatination
        liquor_crime = doc.entity('dat:biken_riken#liquor-crime', {prov.model.PROV_LABEL:'record of all crimes and liquor filtered and unionized', prov.model.PROV_TYPE:'ont:DataSet'})
                  
        doc.wasAttributedTo(liquor_crime, this_script)
        doc.wasGeneratedBy(liquor_crime,get_crime_liquor, endTime)
                  
        # this change this
        doc.wasDerivedFrom(liquor_crime, resource_two,  get_crime_liquor,  get_crime, get_liquor_license)
                  
        repo.logout()
                  
        return doc

## eof

