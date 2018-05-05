
#!/usr/bin/env python
# -*- coding: utf-8 -*-



keras_model = "deep_nn_weights.h5"
model_name = 'tweet_word2vec.model'
# static names 
dataset_location = './Sentiment Analysis Dataset.csv'
model_location = './model/'
tokenized_corpus_name = "tokenized_tweet_corpus.dill"
groun_truth_name  = 'ground_truth_tokenized_tweet_corpus.dill'
model_name = 'tweet_word2vec.mode'


import pandas as pd
import numpy as np
import dill
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split


# Load all data
print("Loading Data")
with open(model_location + tokenized_corpus_name, 'rb') as f:
    tokenized_corpus = dill.load(f)

with open(model_location + groun_truth_name, 'rb') as f:
    ground_truth = dill.load(f)



tfv=TfidfVectorizer(analyzer=lambda x: x, min_df=0, max_features=None, strip_accents='unicode',lowercase =True,
                             token_pattern=r'\w{3,}', ngram_range=(1,1),
                            use_idf=True,smooth_idf=True, sublinear_tf=True, stop_words = "english")   



transformed_data=tfv.fit_transform([x for x in tokenized_corpus] )




X_train, X_test, y_train, y_test = train_test_split(
transformed_data, ground_truth, test_size=0.33, random_state=42)


def batch_generator(X_data, y_data, batch_size):
    samples_per_epoch = X_data.shape[0]
    number_of_batches = samples_per_epoch/batch_size
    counter=0
    index = np.arange(np.shape(y_data)[0])
    while 1:
        index_batch = index[batch_size*counter:batch_size*(counter+1)]
        X_batch = X_data[index_batch,:].toarray()
        y_batch = y_data[index_batch]
        counter += 1
        yield X_batch,y_batch
        if (counter > number_of_batches):
            counter=0







import dill
import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
import keras.backend as K
import multiprocessing
import tensorflow as tf
import numpy as np
from gensim.models.word2vec import Word2Vec
import re
import time
import matplotlib.pyplot as plt
from keras.callbacks import EarlyStopping
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.optimizers import Adam
import time
import multiprocessing
from keras import callbacks
from keras.callbacks import EarlyStopping


use_gpu = True
config = tf.ConfigProto(intra_op_parallelism_threads=multiprocessing.cpu_count(), 
                    inter_op_parallelism_threads=multiprocessing.cpu_count(), 
                    allow_soft_placement=True, 
                    device_count = {'CPU' : multiprocessing.cpu_count(), 
                                    'GPU' : 1 if use_gpu else 0})
session = tf.Session(config=config)
K.set_session(session)



model = Sequential()
# Dense(64) is a fully-connected layer with 64 hidden units.
# in the first layer, you must specify the expected input data shape:
# here, 20-dimensional vectors.
model.add(Dense(64, activation='relu', input_dim=transformed_data.shape[1]))
#model.add(Dropout(0.5))
#model.add(Dense(64, activation='relu'))
#model.add(Dropout(0.5))
#model.add(Dense(64, activation='tanh'))
model.add(Dense(64, activation='tanh'))
model.add(Dense(1, activation='sigmoid'))
# Compile the model
model.compile(loss='binary_crossentropy',
          optimizer=Adam(lr=0.001, decay=1e-6),
          metrics=['accuracy'])


model.fit_generator(generator=batch_generator(X_train, np.array(y_train), 128),
                    nb_epoch=5,
                    validation_data=batch_generator(X_test ,np.array(y_test), 128 ),
                    validation_steps=X_test.shape[0],
                    samples_per_epoch=X_train.shape[0]//32, 
                    max_queue_size=5, 
                    workers=multiprocessing.cpu_count(), 
                    
                    callbacks=[EarlyStopping(min_delta=0.00025, patience=2)])

score = model.evaluate(transformed_data, np.array(ground_truth), batch_size=128)
print("Accuracy: {}".format(score))
print("Model complete, saving")
model.save_weights('tfidf_model.h5')
model.save("tf_idf_full.h5")
