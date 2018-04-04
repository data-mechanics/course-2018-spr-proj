# This is the primary file for crawling tweets via twitters tweetpy api

# Imports. Tweetpy required. 
from tweepy import (Stream, OAuthHandler)
from tweepy.streaming import StreamListener
import pandas as pd
import numpy as np
import os
import os.path as path
import sys

class DataLog:
    """
    Param class 
    Detects duplicate tweets
    """
    unique_users = {}
    unique_tweets = {}
    save_path =  "./TWEETS/"
    fetched_tweets_filename = "tweet_stream.csv"
    # Crawl Count 
    CC = 0

    


class Listener(StreamListener):
    #counter
    tweet_counter = 0 

    def login(self):
        # 2 levels out

        two_up =  path.abspath(path.join(__file__ ,"../../.."))
        auth_df = eval(open( two_up + "/auth.json").read())
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


print("Crawl Begining")
listener = Listener()
# bounding box for boston: -71.1912, 42.2279, -70.8085, 42.3973

listener.getTweetsByGPS(np.inf, -71.1912, 42.2279, -70.8085, 42.3973) 

print(DataLog.unique_users, DataLog.unique_tweets)




