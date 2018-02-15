import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid

class schoolIndex(dml.Algorithm):
    contributor = 'biken_riken'
    reads = []
    writes = ['biken_riken.school_db']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('biken_riken', 'biken_riken')
        
        # Dataset01
        url = 'http://datamechanics.io/data/bm181354_rikenm/Public_Schools.csv'
        #'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0'
        
        school_df = pd.read_csv(url)
        
        #  TODO : bring this from index source
        school = school_df['CITY']
        # (city , no# of school in the area)
        school_df = school_df[school_df['CITY'].apply(lambda l: l).isin(school)].CITY.value_counts()
        # school index
        avg = school_df/school_df.mean()
        max_num = school_df.max()
        # index from 1 to 5
        school_index = (school_df/max_num)*5


        # create new dataframe and concatinate two list
        n = pd.DataFrame(
                 {
                 'school':school_df,
                 'school_index' :school_index
                 })

        r = json.loads(n.to_json( orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)


        # clear
        repo.dropPermanent("school_db")
        repo.createPermanent("school_db")
        #jup_repo.createPermanent('trail_index')
        repo['school_db'].things.insert_many(r)

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
        
        ####
        repo.authenticate('biken_riken', 'biken_riken')
        doc.add_namespace('alg', 'http://datamechanics.io/?prefix=bm181354_rikenm/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp','http://datamechanics.io/?prefix=bm181354_rikenm/')
        
        this_script = doc.agent('alg:biken_riken#schoolIndex', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        ####
        
        #change this [format]
        resource = doc.entity('bdp:Public_Schools', {'prov:label':'dataset of all the schools in the Greater Boston area', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        ###
        
        get_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_school, this_script)
        
        #change this [format]
        doc.usage(get_school, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
                  
        # change this
        index = doc.entity('dat:biken_riken#school_db', {prov.model.PROV_LABEL:'index  of school of boston', prov.model.PROV_TYPE:'ont:DataSet'})
                  
        doc.wasAttributedTo(index, this_script)
        doc.wasGeneratedBy(index, get_school, endTime)
        doc.wasDerivedFrom(index, resource, index, index, index)
                  
        repo.logout()
        return doc

## eof


