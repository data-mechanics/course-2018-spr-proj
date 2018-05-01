import keras.backend as K
from keras.models import Sequential
from keras.layers import Dense
from keras.models import model_from_json
from keras.callbacks import EarlyStopping
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.optimizers import Adam
import os
import tensorflow as tf

import multiprocessing
# Imports. Tweetpy required. 
from tweepy import (Stream, OAuthHandler)
from tweepy.streaming import StreamListener
import pandas as pd
import numpy as np
import os
import os.path as path
import sys
from gensim.models.word2vec import Word2Vec
from nltk.stem.lancaster import LancasterStemmer
from nltk.tokenize import RegexpTokenizer
import re

class CLF:
    model = None
    w2vec = None
    X_vecs = None
    vector_size = None
    max_tweet_length = None


def tokenize_vectorize(X_vecs, tweet, vector_size,max_tweet_length ):
    punctuation = re.compile(r'[><%^*-.?!,":;()|0-9]')
    clean_tweet = punctuation.sub("", tweet.lower()).strip()
    tkr = RegexpTokenizer('[a-zA-Z0-9@]+')
    stemmer = LancasterStemmer()
    tokens = [stemmer.stem(t) for t in tkr.tokenize(tweet) if not t.startswith('@')]
    feature_vec = np.zeros((1, max_tweet_length, vector_size), dtype=K.floatx())

    for t, token in enumerate(tokens):
        if t >= max_tweet_length:
            break
        # Ger X vals
        if token not in X_vecs:
            continue
        feature_vec[:,t, :] = X_vecs[token]
    return feature_vec



def build_model(max_tweet_length, vector_size):
    
    batch_size = 512
    nb_epochs = 5
    model = Sequential()
# This fucking block will eat all of your goddamn memory 
# Sacrifice 3 chickens to improve accuracy 

    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same', input_shape=(max_tweet_length, vector_size)))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Dropout(0.25))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Dropout(0.25))
    model.add(Flatten())
    model.add(Dense(256, activation='tanh'))
    model.add(Dense(2, activation='softmax'))
    # Compile the model
    model.compile(loss='categorical_crossentropy',
              optimizer=Adam(lr=0.001, decay=1e-6),
              metrics=['accuracy'])
    return model

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
        tweet = tmp_df['text'].values[0]
        print(tweet)
        vec = tokenize_vectorize(CLF.X_vecs, tweet, CLF.vector_size, CLF.max_tweet_length )
        probs = CLF.model.predict_proba(vec)
        print("Tweet Recieved: {} with probability {} ".format(tweet, probs))
        # log instances of new users 
        #print("Crawl index:{}".format(DataLog.CC))
        
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
    max_tweet_length = 30
    vector_size = 512
    # generate model 
    use_gpu = True
    config = tf.ConfigProto(intra_op_parallelism_threads=multiprocessing.cpu_count(), 
                            inter_op_parallelism_threads=multiprocessing.cpu_count(), 
                            allow_soft_placement=True, 
                            device_count = {'CPU' : multiprocessing.cpu_count(), 
                                            'GPU' : 1 if use_gpu else 0})

    session = tf.Session(config=config)
    K.set_session(session)
    model = build_model(max_tweet_length, vector_size)
    #test_X = np.load('./test.npz')
    #test_Y = np.load('./test.npz')
    #print("Generating Test Predictions ")
    #probs = model.predict_proba(Test_X)
    #print("Saving Probability Predictions")
    #np.savez_compressed("/proba_preds", probs)
    # static names 
    model_location = './model/'
    keras_model = "deep_nn_weights.h5"
    model_name = 'tweet_word2vec.model'

    model.load_weights(keras_model)
    word2vec = Word2Vec.load(model_location + model_name)
    X_vecs = word2vec.wv

    CLF.model = model
    CLF.w2vec = word2vec
    CLF.X_vecs = X_vecs
    CLF.vector_size = vector_size
    CLF.max_tweet_length = max_tweet_length
    print("Crawl Begining")
    listener = Listener()
    # bounding box for boston: -71.1912, 42.2279, -70.8085, 42.3973

    listener.getTweetsByGPS(np.inf, -71.1912, 42.2279, -70.8085, 42.3973) 

    print(DataLog.unique_users, DataLog.unique_tweets)


if __name__== "__main__":
    main()

