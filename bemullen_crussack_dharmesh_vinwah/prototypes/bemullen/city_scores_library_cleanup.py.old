# Library data clean up 
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas
import math
import urllib.request
from urllib.request import quote 
import json
import csv
import dml
import prov.model
import datetime
import uuid
import xmltodict

class city_scores_library_cleanup(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    # change this reference everywhere else too
    writes = ["bemullen_crussack_dharmesh_vinwah.libraries"] 

    @staticmethod
    def parseURL(url):
        return quote(url,safe='://*\'?=')
    
    @staticmethod
    def execute(trial = False):
        '''retrieve library dataset from city scores dataset'''
        startTime = datetime.datetime.now()

        # set up mongo database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah','bemullen_crussack_dharmesh_vinwah')

        key = "libraries"
        

        d = pandas.read_csv('rptcityscoresummary.csv') 

        data_new = d[d['CTY_SCR_NAME'] == 'LIBRARY USERS']

        data_new = data_new[['CTY_SCR_NBR_DY_01','CTY_SCR_NBR_WK_01','ETL_LOAD_DATE',\
        'CTY_SCR_WEEK','CTY_SCR_DAY','CTY_SCR_DAY_NAME',]]


        def date_cleaner(row):
            date = str(row['ETL_LOAD_DATE'])
            return date[:10]

        data_new['ETL_LOAD_DATE'] = data_new.apply(lambda row: date_cleaner(row),axis=1)

        def session(row): # expand to bucketing if students are in session or not!
            date = row['ETL_LOAD_DATE']
            month = date[5:7]
            day = date[8:10]
                
            if month in ['01']: #january
                if day in ['01','02','03','04','05','06','07','08','09','10','11']:
                    return 0
                else:
                    return 1
            elif month in ['12','05']: #december and may
                if day in ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18']:
                    return 1
                else:
                    return 0  
            elif month in ['06','07','08']:
                return 0
                    
            elif month in ['09','10','11','06','02','03','04',]:
                return 1
            else:
                return 0
        data_new['students'] = data_new.apply(lambda row: session(row),axis=1 )

        student = data_new[data_new['students'] == 1]
        none = data_new[data_new['students'] == 0]
        # how are we going to handle json files
        jsonStu = open('student.json','w')
        jsonNon = open('none.json','w')
        fieldnames = ('CTY_SCR_DAY','CTY_SCR_NBR_DY_01')

        reader = csv.DictReader( student,fieldnames)

        for row in reader:
            json.dump(row,jsonStu)
            jsonfile.write('\n')

        #student_ = student[['CTY_SCR_DAY','CTY_SCR_NBR_DY_01']]
        #none_ = none[['CTY_SCR_DAY','CTY_SCR_NBR_DY_01']]
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(),startTime= None,endTime=None):
        ''' 
            Create the provenance document describing everything happening in this 
            script. Each run of the script will generate a new document describing that
            innocation event.
            '''
         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#city_scores_library_cleanup',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_libraries = doc.entity('bdpm:5bce8e71-5192-48c0-ab13-8faff8fef4d7',
            {'prov:label':'Libraries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_libraries = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve Library Attendence metrics from CityScores dataset'})
        doc.wasAssociatedWith(get_libraries, this_script)

        doc.usage(get_libraries, resource_libraries, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  })

        cityscores = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#Libraries', {prov.model.PROV_LABEL:'Libraries Metrics',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cityscores, this_script)
        doc.wasGeneratedBy(cityscores, get_cityscores, endTime)
        doc.wasDerivedFrom(cityscores, resource_cityscores, get_cityscores,
            get_cityscores, get_cityscores)

        repo.logout()
                  
        return doc


# ----------
# uncomment these to save CSV:
# ----------

# What I need to do is go through and make sure that I have this in the standard format with DML 
# (look at RetrieveCityScores.py between key =... and ...repo.logout(). Also, upload the ds non and student to 
# cspeople/dharmesh/cs591 so that they are up ! )
