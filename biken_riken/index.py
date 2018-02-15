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
    writes = ['biken_riken.indexdb']
    
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
        
        # index of 1 to 5 for transportation
        index_transport = (index_transport/max(index_transport)*5)
        
        # Benchmark and ideal housing cost
        average_housing = arr_housing.mean()
        # applies value/average to all the row of data
        index_house = arr_housing/average_housing
        
        # index 1 to 5 for housing
        index_house = (index_house/max(index_house)*5)
        
        

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
        repo.dropPermanent('indexdb')
        #repo.create_collection("trail_index")
        repo.createPermanent('indexdb')
        repo['indexdb'].insert_many(r)
        
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
        doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/?prefix=bm181354_rikenm/')
        
        this_script = doc.agent('alg:biken_riken#index', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:htaindex_data_places_25', {'prov:label':'dataset of all indices raw values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_index = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_index, this_script)
        
        #change this
        doc.usage(get_index, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        
        # change this
        index = doc.entity('dat:biken_riken#indexdb', {prov.model.PROV_LABEL:'index  of transportation, housing', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(index, this_script)
        doc.wasGeneratedBy(index, get_index, endTime)
        doc.wasDerivedFrom(index, resource, index, index, index)
        
        repo.logout()
        return doc

## eof

