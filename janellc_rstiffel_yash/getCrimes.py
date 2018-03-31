import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import pandas as pd
import time





'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
startTime = datetime.datetime.now()

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')

repo.dropCollection("crimesData")
repo.createCollection("crimesData")


n = 0
df = pd.DataFrame()
startTime = time.time()
while True:

    url = 'https://data.boston.gov/api/3/action/datastore_search?offset='+str(n)+'&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'  
    response = requests.get(url)
    r = response.json()['result']['records']
    s = json.dumps(r, sort_keys=True, indent=2)
    df = pd.concat([df, pd.read_json(s)], axis = 0)
    repo['janellc_rstiffel_yash.crimesData'].insert_many(r)

    if n == 2000:
        break
    n=n+100


repo['janellc_rstiffel.bikePerTract'].metadata({'complete':True})
print(repo['janellc_rstiffel.bikePerTract'].metadata())

repo.logout()



endTime = time.time()
print('Time taken to parse the file: ' + str(endTime - startTime))

df.reset_index(drop=True).to_csv('crime_data.csv')