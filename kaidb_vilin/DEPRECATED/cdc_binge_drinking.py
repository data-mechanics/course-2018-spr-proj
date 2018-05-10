# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 11:36:17 2018

@author: Vasily
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 16:40:22 2018

@author: Vasily
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid

class cdc_binge_drinking(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.payroll']
    DEBUG = True


    @staticmethod
    def clean_ds_ee(df):
        """
        - @args: df -- dataframe containing csv formated cdc binge drinking data
        - @returns  -- df
        """
        #No cleaning needed
        return df
        
    @staticmethod
    def perform_transformations(ds_cdc):
        # Choose rows with Boston in 'City'
        list_ = []
        for i in range(ds_cdc.shape[0]):
            loc = ds_cdc.iloc[i]
            if loc['CityName'] != 'Boston':
                list_.append(i)
        ds_cdc_bos = ds_cdc.drop(list_)
        ds_cdc_bos.reset_index(drop=True,inplace=True)
        slice_bos = ds_cdc_bos[['DataSource', 'Data_Value_Type', 'Data_Value',
       'Low_Confidence_Limit', 'High_Confidence_Limit', 'PopulationCount',
       'GeoLocation', 'TractFIPS']]
        # Find 10 tracts with the worst binge drinking problem
        largest = slice_bos["Low_Confidence_Limit"].nlargest(n = 10, keep = 'first')
        
        #join to get full info on the 10 tracts with the worst binge drinking habits
        joined = pd.DataFrame(largest).join(slice_bos, how = 'inner', lsuffix='_largest', rsuffix='_other')
        return joined
        
    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')
        
        # Retrieve the monthly utility bills and the employee earnings report datasets:
        url = r'https://chronicdata.cdc.gov/api/views/gqat-rcqz/rows.csv?accessType=DOWNLOAD&bom=true&format=true'
        
        ds_cdc = pd.read_csv(url)
        
        # Obj transformation: 
        result = cdc_binge_drinking.perform_transformations(ds_cdc)
        #  df --> string (json formated) --> json 
        r = json.loads(result.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("cdc_binge_drinking")
        repo.createCollection("cdc_binge_drinking")

        #if cdc_binge_drinking.DEBUG:
         #   print(type(r))
          #  assert isinstance(r, dict)

        repo['kaidb_vilin.cdc_binge_drinking'].insert_many( r)

        repo['kaidb_vilin.cdc_binge_drinking'].metadata({'complete':True})
        print(repo['kaidb_vilin.cdc_binge_drinking'].metadata())

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
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dbd', 'https://data.boston.gov/dataset/')

        this_script = doc.agent('alg:kaidb_vilin#cdc_binge_drinking', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:wc8w-nujj', {'prov:label':'cdc-binge-drinking', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_cdc_binge_drinking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cdc_binge_drinking, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_cdc_binge_drinking, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'418983dc-7cae-42bb-88e4-d56f5adcf869/resource/8368bd3d-3633-4927-8355-2a2f9811ab4f/download/employee-earnings-report-2016.csv'
                  }
                  )
     

        cdc_binge_drinking = doc.entity('dat:kaidb_vilin#cdc_binge_drinking', {prov.model.PROV_LABEL:'cdc_binge_drinking', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cdc_binge_drinking, this_script)
        doc.wasGeneratedBy(cdc_binge_drinking, get_cdc_binge_drinking, endTime)
        doc.wasDerivedFrom(cdc_binge_drinking, resource, get_cdc_binge_drinking, get_cdc_binge_drinking, get_cdc_binge_drinking)
        repo.logout()
                  
        return doc

# comment this out for submission. 
'''cdc_binge_drinking.execute()
doc = cdc_binge_drinking.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))'''

## eof
