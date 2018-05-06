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
from tqdm import tqdm 
import sys




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


def classify_tweet(tweet, likes):
    #print()
    # Retrieve tweet it
    hashtags = re.findall(r"#(\w+)", str ( tweet) )
    
    vec = tokenize_vectorize(Model.vect, tweet, )
    # I only care about th efirst 1 
    probs =Model.clf.predict_proba(vec)[:,1]
    pred = Model.clf.predict(vec)
    
    w_probs = probs * int(likes)
    w_pred = pred * int(likes)

    DataLog.pos_d[pred[0]] +=1
    DataLog.weighted_pos_d[pred[0]] += likes 
    DataLog.pos_d[1] += np.squeeze(probs)
    DataLog.pos_d[0] += 1 - np.squeeze(probs)

    DataLog.weighted_pos_d[1] += np.squeeze(w_probs)
    DataLog.weighted_pos_d[0] += 1 - np.squeeze(w_probs)
    if len(hashtags) >0:
        for ht in hashtags:
            if ht not in DataLog.hash_tags:
                DataLog.hash_tags[ht] = {0:0,1:0}

            DataLog.hash_tags[ht][1] += np.squeeze(probs)
            DataLog.hash_tags[ht][0] += 1- np.squeeze(probs)

   
    DataLog.CC +=1
    #if DataLog.CC %1000 ==0:
        #print("Crawl index:{}".format(DataLog.CC))
    return list(probs.flatten()), list(pred.flatten()), [hashtags]



def main():
    # most current model 
    # load in
    config = eval(open( '../config.json').read())
    model_name = config['Model name'] 
    vectorizer_name = config["Vectorizer name"]
    # most current model 
    model_path = 'model/{}'.format(model_name)
    vectorize_path =  'model/{}'.format(vectorizer_name)
    try:
        data_path = sys.argv[1]
        assert(".csv" in data_path)
    except:
        print("Error: please specify a data path")
        print("For example: python classify_tweet_dataset.py  ~/Documents/mass_twitter.csv")
        print("EXITING ")
        return -1
    df = pd.read_csv(data_path)

    data_loc = "./Data/"
    sent_tweets = data_loc + 'Sent_Tweets/'
    DataLog.save_path = sent_tweets
    clf = joblib.load(model_path)
    print("Loaded Model")
    print("Crawl Begining")
    tf_vect = pickle.load(open(vectorize_path, "rb"))
    Model.clf = clf
    Model.vect = tf_vect
    print("loading data")
    
    text = df.text.values
    fav = df.favorite_count.values
    preds = []
    probs = []
    hashtags = []
    for i in tqdm(range(len(text))):
        try:
            proba, pred, hts = classify_tweet(text[i], fav[i])
            preds += pred
            probs += proba
            hashtags += hts
        except:
            
            print("error encountered at {}: text: {} fav:{}".format(i, text[i], fav[i]))
            preds += [-1]
            probs += [-1]
            hashtags += [[]]

    print("Finished clasisfying. ")
    preds = np.array(preds)
    probs = np.array(probs)
    print("Building new Dataframe")

    df['preds'] = preds
    df['w_preds'] = preds * fav

    df['pos_proba'] =  probs
    df['w_pos_proba'] = probs * fav
    df['hashtags'] = hashtags

    print("writting CSV")
    df.to_csv("mass_sentiment_data.csv")

    with open('hashtags_mass.pkl', 'wb') as file:
            file.write(pickle.dumps(DataLog.hash_tags) )

    tmp_stats = [ DataLog.pos_d, 
        DataLog.pos_prob_d , 
        DataLog.weighted_pos_d,
        DataLog.weight_prob_d
        ]

    with open('sent_stats_mass.pkl', 'wb') as file:
        file.write(pickle.dumps(tmp_stats) )


    


if __name__== "__main__":
    main()

