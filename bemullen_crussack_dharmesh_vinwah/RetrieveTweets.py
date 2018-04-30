# Filename: RetrieveTweets.py
# Authors: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Retrieves tweets and formats them appropriately before
#              inserting them into the database.
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import requests

class RetrieveTweets(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.tweets"]

    @staticmethod
    def parseURL(url):
        return quote(url, safe='://*\'?=')

    @staticmethod
    def parseTweetJSON(response):
        response = response.replace("\n", ",")
        response = "[" + response
        response = response + "]"
        response = list(response)
        if response[-2] == ",":
            response[-2] = ""
        response = "".join(response)
        return response

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        key = "tweets"
        base_url = "https://cs-people.bu.edu/dharmesh/cs591/591data/"
        url_stems = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

        if trial:
            url = base_url + url_stems[0] + ".json"
            r = json.loads(RetrieveTweets.parseTweetJSON(response))
            repo.dropCollection(key)
            repo.createCollection(key)
            repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many(r)
        else:

            collected = {}
            for w in url_stems:
                url = base_url + w + ".json"
                response = requests.get(url).text
                collected[w] = response

            repo.dropCollection(key)
            repo.createCollection(key)

            for w in url_stems:
                operate = collected[w]
                operate = operate.replace("\n", ",")
                operate = "[" + operate
                operate = operate + "]"
                operate = list(operate)
                if operate[-2] == ",":
                    operate[-2] = ""
                operate = "".join(operate)
                operate = json.loads(operate)
                repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many(operate)
                collected[w] = operate
            
            # for c in collected:
            #     repo['bemullen_crussack_dharmesh_vinwah.' + key].insert_many(c)

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
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')

        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#RetrieveTweets',\
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_tweets = doc.entity('csdt:tweets',
            {'prov:label':'[jan, feb, mar, apr,\
            may, jun, jul,aug,sep,oct,nov,dec]',
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_tweets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
            {'prov:label': 'Retrieve Tweets About Boston City from period 2016-01-01 to 2016-12-31'})
        doc.wasAssociatedWith(get_tweets, this_script)
        doc.usage(get_tweets, resource_tweets, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        tweets_retrival = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#tweets',\
            {prov.model.PROV_LABEL:'Tweets about Boston City during the year 2016',
            prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(tweets_retrival, this_script)
        doc.wasGeneratedBy(tweets_retrival, get_tweets, endTime)
        doc.wasDerivedFrom(service_requests, resource_tweets, get_tweets,
            get_tweets, get_tweets)

        repo.logout()
                  
        return doc