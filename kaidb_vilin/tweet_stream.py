
import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
import sys
import urllib.request
import gzip
from tweepy import (Stream, OAuthHandler)
from tweepy.streaming import StreamListener
import pandas as pd
import numpy as np
import os
import os.path as path
import sys


import requests, zipfile, io

class tempTweets:
    tweets = None


class Listener(StreamListener):
    #counter
    tweet_counter = 0 

    def login(self):
        # 2 levels out

        two_up =  path.abspath(path.join(__file__ ,"../"))
        auth_df = eval(open( two_up + "auth.json").read())
        CONSUMER_KEY = auth_df['consumer_key']
        CONSUMER_SECRET = auth_df['consumer_secret']
        ACCESS_TOKEN = auth_df['access_key']
        ACCESS_TOKEN_SECRET = auth_df['access_secret']
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        return auth

    def on_status(self, status):
        Listener.tweet_counter += 1
        #print(status._json)
        tmp_df = pd.DataFrame([status._json])
        tempTweets.tweets = tmp_df
        # Retrieve tweet it
        tmp_tweet_id = tmp_df.id.values[0]
            
        # Get the unique identifier for the user
        tmp_user = tmp_df.user.values[0]['id']
        # log instances of new users 
        if tmp_user not in DataLog.unique_users:
            DataLog.unique_users[tmp_user] = True

        # log and save instances of unique tweets
        if tmp_tweet_id not in DataLog.unique_tweets:
            DataLog.unique_tweets[tmp_tweet_id] = True
            save_name =  str("user_{}_tweetID_{}_".format(tmp_user, tmp_tweet_id)) + DataLog.fetched_tweets_filename
            # Write by 
            tmp_df.to_csv(DataLog.save_path + save_name)
            print("succesfully wrote  tweet {}".format(tmp_tweet_id))
        DataLog.CC +=1
        print("Crawl index:{}".format(DataLog.CC))
        
        if Listener.tweet_counter < Listener.stop_at:
            return True
        else:
            print('Max num reached = ' + str(Listener.tweet_counter))
            return False

    def getTweetsByGPS(self, stop_at_number, latitude_start, longitude_start, latitude_finish, longitude_finish):
        try:
            Listener.stop_at = stop_at_number # Create static variable
            auth = self.login()
            streaming_api = Stream(auth, Listener(), timeout=60) # Socket timeout value
            streaming_api.filter(follow=None, locations=[latitude_start, longitude_start, latitude_finish, longitude_finish])
        except KeyboardInterrupt:
            print('Got keyboard interrupt')

    def getTweetsByHashtag(self, stop_at_number, hashtag):
        try:
            Listener.stopAt = stop_at_number
            auth = self.login()
            streaming_api = Stream(auth, Listener(), timeout=60)
            # Atlanta area.
            streaming_api.filter(track=[hashtag])
        except KeyboardInterrupt:
            print('Got keyboard interrupt')






class tweet_stream(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.']
    DEBUG = False

def download_zip(zip_file_url):
    print("Downloading the zip file at {}".format(zip_file_url))
    r = requests.get(zip_file_url)
    print("Extracting contents to local directory")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    

    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')

        # location of twitter data
        print("Crawl Begining")
        listener = Listener()
        # bounding box for boston: -71.1912, 42.2279, -70.8085, 42.3973
        # get 1 tweet for demonstrative purposes 
        listener.getTweetsByGPS(1, -71.1912, 42.2279, -70.8085, 42.3973) 
        data = tempTweets.tweets
        # Obj transformation: 
        #  df --> string (json formated) --> json 
        r = json.loads(data.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        # formated string. 
        #s = json.dumps(r, sort_keys=True, indent=2)

        ######

        repo.dropCollection("tweet_stream")
        repo.createCollection("tweet_stream")

        repo['kaidb_vilin.tweet_stream'].insert_many(r)

        
        repo['kaidb_vilin.tweet_stream'].metadata({'complete':True})
        print(repo['kaidb_vilin.tweet_stream'].metadata())

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
        doc.add_namespace('dbd', 'https://developer.twitter.com/en/docs')
        doc.add_namespace('rc', 'tweepy')

        this_script = doc.agent('alg:kaidb_vilin#tweet_stream', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_tweet_stream = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_tweet_stream, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_tweet_stream, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'tweepy.retrieve'
                  }
                  )
     

        tweet_stream = doc.entity('dat:kaidb_vilin#tweet_stream', {prov.model.PROV_LABEL:'tweet_stream', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tweet_stream, this_script)
        doc.wasGeneratedBy(tweet_stream, get_tweet_stream, endTime)
        doc.wasDerivedFrom(tweet_stream, resource, get_tweet_stream, get_tweet_stream, get_tweet_stream)
        repo.logout()
                  
        return doc

# comment this out for submission. 
tweet_stream.execute()
doc = tweet_stream.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
