"""
Tweepy python script used to retrieve all tweets from a particular user
@Author: Kai Bernardini

Tweepy -- https://github.com/tweepy/tweepy
adapted from. -- (TODO: link to github)
"""
import tweepy 
# I/O read/writes
import pandas as pd
import os.path as path
import sys

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

#Twitter API credentials
# Do not leave values hardcoded 


two_up =  path.abspath(path.join(__file__ ,"../../.."))
auth_df = eval(open( two_up + "/auth.json").read())
print(auth_df.keys())
consumer_key = auth_df['consumer_key']
consumer_secret = auth_df['consumer_secret']
access_key = auth_df['access_key']
access_secret = auth_df['access_secret']
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
#print(auth_df)
print("Authorizing")

def get_all_tweets(screen_name, use_pandas = False):
    """Retrieve all tweets froma. particular users by their username
    - Notes: Twitter will on ly store the last 3,240 tweets from a particular 
    user using standard Dev creds. """
    
    # Authorization and initialization
   
    
    #initialize a  dumb-list to hold scraped tweets
    alltweets = []  
    # Get first 200 tweets 
    new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    #save the id of the oldest tweet less one
    # This tells us where to begin our search 
    oldest = alltweets[-1].id - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print( "getting tweets before %s" % (oldest))
        
        #all subsiquent requests use the max_id param to prevent duplicates
        # We can only get at most 200 tweets per querry
        # BONUS: twitter doesn't appear to limit this. 
        # Make sure caching is enabled as to not prevent duplicate querries 
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
        print( "...{} tweets downloaded so far".format(len(alltweets)))
    if use_pandas:
        AT = [alltweets[i]._json for i in range(len(alltweets))]
        data = pd.DataFrame(AT)
        return data 

    return alltweets





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




def main(df, user):
    
    data_loc = "./Data/"
    save_path = './user_tweets/{}'.format(user) + "/"

    os.makedirs(save_path)
    sent_tweets = data_loc + 'Sent_Tweets/'
    DataLog.save_path = sent_tweets
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
    df.to_csv(save_path +"{}_sentiment_data.csv".format(user))

    with open(save_path + '{}_hashtags.pkl'.format(user), 'wb') as file:
            file.write(pickle.dumps(DataLog.hash_tags) )

    tmp_stats = [ DataLog.pos_d, 
        DataLog.pos_prob_d , 
        DataLog.weighted_pos_d,
        DataLog.weight_prob_d
        ]

    with open(save_path +'{}_sent_stats.pkl'.format(user), 'wb') as file:
        file.write(pickle.dumps(tmp_stats) )



if __name__ == '__main__':
    model_path = 'model/l2_LR1524414744.4756281.pkl'
    vectorize_path =  'model/vectorizer1524414744.4756281.pk'
    clf = joblib.load(model_path)
    tf_vect = pickle.load(open(vectorize_path, "rb"))
    Model.clf = clf
    Model.vect = tf_vect
    print("Loaded Model: {}".format(sys.argv))
    for i in tqdm(range(len(sys.argv))):
        user = sys.argv[i]
        # bizzaire need to hack this. 
        #HACK 
        if '.py' in user:
            continue
        print("Building sentiment dataset for user {}".format(user))

        try:
            if os.path.isdir('./user_tweets/{}'.format(user) ):
                print("already retrieved history for {}".format(user))
                continue
            df = get_all_tweets(user, use_pandas=True)
            print("Built Data for {}".format(user))
        except Exception as e:
            print("Error: please specify a twitter username {}, {} given".format(e, user))
            continue
        print("Dataset built. Now  loading model ")
        main(df, user)


#test()
