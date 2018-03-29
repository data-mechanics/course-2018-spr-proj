import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import pandas as pd
import time

n = 0
df = pd.DataFrame()
startTime = time.time()
while True:

    url = 'https://data.boston.gov/api/3/action/datastore_search?offset='+str(n)+'&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'  
    response = requests.get(url)
    r = response.json()['result']['records']
    s = json.dumps(r, sort_keys=True, indent=2)
    df = pd.concat([df, pd.read_json(s)], axis = 0)
    #repo['yash.crimesData'].insert_many(r)

    n = n+100
    if n == 200000:
        break
endTime = time.time()
print('Time taken to parse the file: ' + str(endTime - startTime))

df.reset_index(drop=True).to_csv('crime_data.csv')