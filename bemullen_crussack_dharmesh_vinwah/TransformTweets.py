# Filename: TransformTweets.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import subprocess
import DharmeshDataMechanics.CS591 as Constants

class TransformTweets(dml.Algorithm):
    contributor = Constants.CONTRIBUTOR
    key = "tweets"
    reads = [Constants.BASE_AUTH + "." + key]
    writes = [Constants.BASE_AUTH + "." + key]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(Constants.BASE_AUTH, Constants.BASE_AUTH)

        tweets = repo[Constants.BASE_NAME + "." + "tweets"]
        for tweet in tweets.find():
            tweets.update_one({'_id': tweet['_id']},
                {
                '$set': {
                'datetime': datetime.datetime.strptime(tweet['date'] + ' ' + tweet['time'], '%Y-%m-%d %H:%M:%S')
                }
                })

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(Constants.BASE_AUTH, Constants.BASE_AUTH)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')


        this_script = doc.agent('alg:' + Constants.BASE_NAME + '#TransformTweets',\
            {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        tweets = doc.entity('dat:' + Constants.BASE_NAME + '#tweets',\
            {prov.model.PROV_LABEL:'Tweets about Boston City', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_tweets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Tweets about Boston City posted during 2016'})        
        doc.wasAssociatedWith(get_tweets, this_script)
        doc.used(get_tweets, tweets, startTime)
        doc.wasAttributedTo(tweets, this_script)
        doc.wasGeneratedBy(tweets, get_tweets, endTime)        
        
        repo.logout()
        return doc

# if __name__ == "__main__":
#     TransformTweets.execute()