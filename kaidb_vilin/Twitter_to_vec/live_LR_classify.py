#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

import multiprocessing
# Imports. Tweetpy required. 
from tweepy import (Stream, OAuthHandler)
from tweepy.streaming import StreamListener
import pandas as pd
import numpy as np
import os
import os.path as path
import sys
import dill
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
from nltk.stem.lancaster import LancasterStemmer
from nltk.tokenize import RegexpTokenizer
import re




punctuation = re.compile(r'[><%^*-.?!,":;()|0-9]')
stemmer = LancasterStemmer()
tkr = RegexpTokenizer('[a-zA-Z0-9@]+')


def tokenize_vectorize(tkr , tweet ):
    punctuation = re.compile(r'[><%^*-.?!,":;()|0-9]')
    clean_tweet = punctuation.sub("", tweet.lower()).strip()
    tkr = RegexpTokenizer('[a-zA-Z0-9@]+')
    stemmer = LancasterStemmer()
    # dropping all @mentions 
    tokens = [stemmer.stem(t) for t in tkr.tokenize(tweet) if not t.startswith('@')]

    feature_vector = Model.vect.transform([ " ".join(tokens) ])
    #print(feature_vector.shape)
    return  feature_vector



class Model:
    clf = None
    vect = None 
   


class DataLog:
    """
    Param class 
    Detects duplicate tweets
    """
    unique_users = {}
    unique_tweets = {}
    save_path =  None
    fetched_tweets_filename = "tweet_stream_class.json"
    sent_map = {0:'Negative', 1:'Positive'}

    pos_d = {
    0:0,
     1:0
     }
    pos_prob_d = {
    0:0.0,
     1:0.0
     }

    # weighted by likes 
    weighted_pos_d = {
    0:0,
     1:0
     }

    #weighted by like
    weight_prob_d={
    0:0,
     1:0
     }
    
    # data store 
    hash_tags= {

    }

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
        #print()
        # Retrieve tweet it
        tmp_tweet_id = tmp_df.id.values[0]
        care = ['id_str', 'text', 'coordinates','favorite_count' ,'geo', 'user', 'entities', 'sentiment_proba', 'sentiment_pred']
        # Get the unique identifier for the user
        tmp_user = tmp_df.user.values[0]['id']
        tweet = tmp_df['text'].values[0]
        hashtags = re.findall(r"#(\w+)", str ( tweet) )
        if tmp_df.lang.values[0] != 'en':
            print("Skipping Non-English Tweet")
            return True
        likes = tmp_df['favorite_count'].values[0]
        vec = tokenize_vectorize(Model.vect, tweet, )
        # I only care about th efirst 1 
        probs =Model.clf.predict_proba(vec)[:,1]
        pred = Model.clf.predict(vec)
        
        w_probs = probs * int(likes)
        w_pred = pred * int(likes)

        DataLog.pos_d[pred[0]] +=1
        DataLog.weighted_pos_d[pred[0]] += likes 

        print("Tweet Recieved with probability {}, prediction {}, Ratio, {}, likes {}  with text {} ".format( probs, pred, DataLog.pos_d[1]/ (DataLog.pos_d[1] + DataLog.pos_d[0]) ,likes, tweet))
        #print(tmp_df.shape)
        tmp_df['sentiment_proba'] = np.squeeze(probs)

        tmp_df['sentiment_pred'] = np.squeeze(pred)
        DataLog.pos_d[1] += np.squeeze(probs)
        DataLog.pos_d[0] += 1 - np.squeeze(probs)

        DataLog.weighted_pos_d[1] += np.squeeze(w_probs)
        DataLog.weighted_pos_d[0] += 1 - np.squeeze(w_probs)
        tmp_df['hashtags'] = [ hashtags]
        if len(hashtags) >0:
            for ht in hashtags:
                if ht not in DataLog.hash_tags:
                    DataLog.hash_tags[ht] = {0:0,1:0}

                DataLog.hash_tags[ht][1] += np.squeeze(probs)
                DataLog.hash_tags[ht][0] += 1- np.squeeze(probs)

        if tmp_user not in DataLog.unique_users:
            DataLog.unique_users[tmp_user] = True

        # log and save instances of unique tweets
        if tmp_tweet_id not in DataLog.unique_tweets:
            DataLog.unique_tweets[tmp_tweet_id] = True
            save_name =  str("user_{}_tweetID_{}_".format(tmp_user, tmp_tweet_id)) + DataLog.fetched_tweets_filename
            # Write by 
            tmp_df[care].iloc[0].to_json(DataLog.save_path + save_name)

            print("succesfully wrote  tweet {}".format(tmp_tweet_id))
        DataLog.CC +=1
        if DataLog.CC %10 ==0:
            with open('hashtags.pkl', 'wb') as file:
                file.write(pickle.dumps(DataLog.hash_tags) )
            tmp_stats = [ DataLog.pos_d, 
             DataLog.pos_prob_d , 
             DataLog.weighted_pos_d,
             DataLog.weight_prob_d]

            with open('sent_stats.pkl', 'wb') as file:
                file.write(pickle.dumps(tmp_stats) )

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


def main():
    config = eval(open( '../config.json').read())
    model_name = config['Model name'] 
    vectorizer_name = config["Vectorizer name"]
    # most current model 
    model_path = 'model/{}'.format(model_name)
    vectorize_path =  'model/{}'.format(vectorizer_name)
    data_loc = "./Data/"
    sent_tweets = data_loc + 'Sent_Tweets/'
    DataLog.save_path = sent_tweets
    clf = joblib.load(model_path)
    print("Loaded Model")
    print("Crawl Begining")
    tf_vect = pickle.load(open(vectorize_path, "rb"))
    Model.clf = clf
    Model.vect = tf_vect
    with open('../Twitter_to_vec/sent_stats.pkl', 'rb') as f:
        tmp = pickle.load(f)

    DataLog.pos_d = tmp[0]
    DataLog.pos_prob_d = tmp[1]
    DataLog.weighted_pos_d = tmp[2]
    DataLog.weight_prob_d = tmp[3]

    with open('../Twitter_to_vec/hashtags.pkl', 'rb') as f:
        ht= pickle.load(f)
    DataLog.hash_tags = ht
    listener = Listener()
    # bounding box for boston: -71.1912, 42.2279, -70.8085, 42.3973

    listener.getTweetsByGPS(np.inf, -71.1912, 42.2279, -70.8085, 42.3973) 

    print(DataLog.unique_users, DataLog.unique_tweets)


if __name__== "__main__":
    main()

