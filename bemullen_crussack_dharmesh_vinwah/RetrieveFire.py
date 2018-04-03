# Filename: RetrieveFire.py
# Author: Claire Russack <crussack@bu.edu>

import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import prequest
import geocoder
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np

class RetrieveFire(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.fires"]


    @staticmethod
    def parseURL(url):
        return quote(url, safe='://*\'?=')

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        key = "fires"
        address = {}
        urls = []
        
        urls.append(RetrieveFire.parseURL('https://data.boston.gov/api/3/action/datastore_search?resource_id=14683ec2-c53a-46e0-b6de-67ec123629f0')) #september
        urls.append(RetrieveFire.parseURL('https://data.boston.gov/api/3/action/datastore_search?resource_id=ce5cb864-bd01-4707-b381-9e204b4db73f'))  #december
        urls.append(RetrieveFire.parseURL('https://data.boston.gov/api/3/action/datastore_search?resource_id=9d91dbc7-9875-4cd9-a772-3b363a4b193f'))  #may
        #print(response)
        for url in urls:
            r = json.loads(prequest.get(url).text)
            month = ""
            if url[-1] == '0':
                month = 'september'
            elif url[-3] == '7':
                month = 'december'
            else:
                month = 'may' 

            for record in r['result']['records']:
                streetAddress = record['Street Number'].strip() + " " + record['Street Name'].strip() + " " + record['Street Type'].strip() + " " +  record['Neighborhood'].strip() + "MA " + record['Zip'].strip()
                g = geocoder.google(streetAddress)
                g = geocoder.google(streetAddress)
                address[record['Incident Number']] = (month, g.latlng)

#some long streets don't have an exact number attached to them - this could be a point of error 
        may = []
        sept = []
        dec = []
        for record in address.keys():
            if address[record][1] == None or address[record][1] == 'null':
                record = ""
            elif address[record][0] == 'may':
                may.append(address[record][1])
            elif address[record][0] == 'september':
                sept.append(address[record][1])
            elif address[record][0] == 'december':
                dec.append(address[record][1])
            
        df_may = pd.DataFrame.from_dict(may)
        df_sept = pd.DataFrame.from_dict(sept)
        df_dec = pd.DataFrame.from_dict(dec)


        f1 = df_may[0].values
        f2 = df_may[1].values
        y = list(zip(f1,f2))
        X=np.matrix(y)
        kmeans = KMeans(n_clusters=2)
        kmeans.fit(X)
        print('May Centroids = ', kmeans.cluster_centers_)
        

        f3 = df_sept[0].values
        f4 = df_sept[1].values
        x = list(zip(f3,f4)) 
        W=np.matrix(x)
        kmeans2 = KMeans(n_clusters=2)
        kmeans2.fit(W)
        print('September Centroids = ', kmeans2.cluster_centers_)

        f5 = df_dec[0].values
        f6 = df_dec[1].values
        w = list(zip(f5,f6))
        Z=np.matrix(w)
        kmeans3 = KMeans(n_clusters=2)
        kmeans3.fit(Z)
        print('December Centroids = ', kmeans3.cluster_centers_)

        repo.dropCollection(key)
        repo.createCollection(key)
        repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many([address])

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
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveFire',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_fires = doc.entity('bdp:8f4f497e-d93c-4f2f-b754-bfc69e2700a0',
            {'prov:label':'Fires', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_fires = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve Fire Incidents from September 2017'})
        doc.wasAssociatedWith(get_fires, this_script)
        doc.usage(get_fires, resource_fires, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'''?sql=SELECT * from "5bce8e71-5192-48c0-ab13-8faff8fef4d7" WHERE "ETL_LOAD_DATE" >= '2016-02-01 00:00:00' AND "ETL_LOAD_DATE" <= '2018-01-01 00:00:00' '''
                  })

        fires = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#fires', {prov.model.PROV_LABEL:'Fire Incidents',
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fires, this_script)
        doc.wasGeneratedBy(fires, get_fires, endTime)
        doc.wasDerivedFrom(fires, resource_fires, get_fires,
            get_fires, get_fires)

        repo.logout()
                  
        return doc
#RetrieveFire.execute()
