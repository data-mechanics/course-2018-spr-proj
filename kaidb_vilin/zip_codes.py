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
from sklearn import preprocessing

class zip_codes(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.payroll']
    DEBUG = True


    @staticmethod
    def clean_ds_ee(df):
        """
        - @args: df -- dataframe containing csv formated boston payroll data. 
        - @returns  -- cleaned and formated Boston payroll dataframe.  
        Boston Payroll Data has missing values to indicate an absence of a payment. 
        it also represents paychecks with $DOLLAR_AMOUNT. This function puts the 
        data in a usable formate by replacing NAANs with 0 and $x with x. It then sorts the idnex
        by descending total_pay
        """
        ################# clean data #####################
        # NAN in this case means no money was given
        df = df.fillna(0)
        # Convert dollars to numeric/float
        pay_info = ['REGULAR', 'RETRO', 'OTHER',
                    'OVERTIME', 'INJURED', 'DETAIL', 
                    'QUINN/EDUCATION INCENTIVE', 
                    'TOTAL EARNINGS']
        for col in pay_info:
            df[col]  = df[col].replace('[\$,]', '', regex=True).astype(float).fillna(0)
            
        #rename the zipcode column:
        df = df.rename(columns = {"POSTAL": "Zip"})
        
        # Sort by earnings (descending )
        return df.sort_values("TOTAL EARNINGS", ascending=False)
        ################# End Clean Data ################
        
    @staticmethod
    def perform_transformations(ds_utils, ds_ee):
        # choose only certain columns
        slice_utils = ds_utils[['EnergyTypeName', 'TotalCost', 'TotalConsumption', 'Zip']]
        # Will look only at electricityy bills
        df = slice_utils.drop(slice_utils[slice_utils.EnergyTypeName != 'Electric'].index)
        # Drop outliers and reset index       
        df = df.drop(df[df.TotalConsumption == 0].index)
        df.reset_index(drop = True, inplace = True)
        # Estimate cost per unit of electricity:
        df['CostPerUnit'] = df['TotalCost']/df['TotalConsumption'].values
        # Drop other axis and group by neighborhood aka Zip Code
        df = df.drop(['EnergyTypeName','TotalCost', 'TotalConsumption'], axis = 1)
        grouped_utils = df.groupby(by = 'Zip', as_index = True).mean()

        # Perform similar transformations on the payroll dataset
        zips_utils = {i for i in grouped_utils.index.values}        
        slice_ee = ds_ee[['REGULAR', 'Zip']]       
        grouped_ee = slice_ee.groupby(by = 'Zip', as_index = True).mean()
        # Drop zip codes that do not overlap
        list_ = []
        for i in grouped_ee.index:
            if i not in zips_utils:
                list_.append(i)
        grouped_ee = grouped_ee.drop(list_)
        
        # Now join the two on Zip Code
        grouped = pd.concat([grouped_utils, grouped_ee], axis = 1, copy=False, join='inner')
        result = grouped.copy()
        result['CostPerUnit'] = preprocessing.scale(result['CostPerUnit'])
        result['REGULAR'] = preprocessing.scale(result['REGULAR'])
        # Compute the difference score. The bigger the score for Zip Code X, the better off people in X are
        # in terms of Earnings/Utility costs
        result['DifferenceScore'] = result['REGULAR'] - result['CostPerUnit']
        return result
        
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
        utils_url = r'https://data.boston.gov/dataset/e8c755cb-8ba5-4146-8e40-82fd35a8ef9d/resource/35fad26c-1400-46b0-846c-3bb6ca8f74d0/download/monthlyutilitybills.csv'
        ee_url = r'https://data.boston.gov/dataset/418983dc-7cae-42bb-88e4-d56f5adcf869/resource/8368bd3d-3633-4927-8355-2a2f9811ab4f/download/employee-earnings-report-2016.csv'
        
        ds_utils = pd.read_csv(utils_url, encoding = "ISO-8859-1")
        ds_ee = pd.read_csv(ee_url, encoding = "ISO-8859-1")

        # puts data in useable format 
        ds_ee = zip_codes.clean_ds_ee(ds_ee)
        
        # Obj transformation: 
        result = zip_codes.perform_transformations(ds_utils, ds_ee)
        #  df --> string (json formated) --> json 
        r = json.loads(result.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("zip_codes")
        repo.createCollection("zip_codes")

        #if zip_codes.DEBUG:
         #   print(type(r))
          #  assert isinstance(r, dict)

        repo['kaidb_vilin.zip_codes'].insert_many( r)

        repo['kaidb_vilin.zip_codes'].metadata({'complete':True})
        print(repo['kaidb_vilin.zip_codes'].metadata())

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

        this_script = doc.agent('alg:kaidb_vilin#zip_codes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:wc8w-nujj', {'prov:label':'employee-earnings-report-2016, monthly-utility-bills', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_zip_codes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zip_codes, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_zip_codes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'418983dc-7cae-42bb-88e4-d56f5adcf869/resource/8368bd3d-3633-4927-8355-2a2f9811ab4f/download/employee-earnings-report-2016.csv'
                  }
                  )
     

        zip_codes = doc.entity('dat:kaidb_vilin#zip_codes', {prov.model.PROV_LABEL:'zip_codes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zip_codes, this_script)
        doc.wasGeneratedBy(zip_codes, get_zip_codes, endTime)
        doc.wasDerivedFrom(zip_codes, resource, get_zip_codes, get_zip_codes, get_zip_codes)
        repo.logout()
                  
        return doc

# comment this out for submission. 
'''zip_codes.execute()
doc = zip_codes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))'''

## eof
