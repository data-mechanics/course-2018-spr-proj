
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dill
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
from keras.callbacks import ModelCheckpoint
from sklearn.model_selection import train_test_split
#from sklearn import preprocessing
from keras.layers import Input, Dense, Embedding, Conv2D, MaxPool2D
from keras.layers import Reshape, Flatten, Dropout, Concatenate
from keras.callbacks import ModelCheckpoint
from keras.optimizers import Adam
from keras.models import Model
from sklearn.externals import joblib 
from keras import regularizers


def build_train_valid_test(tokenized_corpus, ground_truth, train_size, validation_size, test_size, X_vecs ):
    # Generate random indexes

    vector_size = 512
    np.random.seed(6969)
    indexes = set(np.random.choice(len(tokenized_corpus), train_size + test_size, replace=False))

    X_train = np.zeros((train_size, 3, vector_size), dtype=K.floatx())
    Y_train = np.zeros((train_size, 2), dtype=np.int32)

    X_valid = np.zeros((validation_size, 3, vector_size), dtype=K.floatx())
    Y_valid = np.zeros((validation_size, 2), dtype=np.int32)


    X_test = np.zeros((test_size, 3, vector_size), dtype=K.floatx())
    Y_test = np.zeros((test_size, 2), dtype=np.int32)


    # Really, really lazy way of doing this. 
    # TODO: Do this in a not so lazy way 

    # Validation is first 10,000 indicies
    # Test is the second 10,000 indicies
    # Train is the rest 
    for i, index in enumerate(indexes):
        scale = len(tokenized_corpus[i])
        mi = np.inf
        ma = 0
        mav = None
        miv= None
        if i %100000 ==0:
            print(i/len(tokenized_corpus))
        
        for t, token in enumerate(tokenized_corpus[index]):
            
            # Ger X vals
            if token not in X_vecs:
                continue
            v = X_vecs[token]
            
            norm = np.linalg.norm(v)
            if norm > ma:
                ma = norm
                mav = v
            if norm < mi and norm !=0:
                m = norm
                miv = v
                
            # add vector to to validation set 
            if i < validation_size:
                X_valid[i, 0, :] += v/(1e-6 + scale)
            # add vector to test set 
            elif  validation_size < i  and i < validation_size + test_size :
                X_test[i - test_size, 0, :] =v/(1e-6 + scale)
            # add to train set 
            else: 
                X_train[i - train_size, 0, :] = v/(1e-6 + scale)
        # 1 hot encoding for y balus      
        if i < validation_size :
            # softmax 1 hot encoding 
            Y_valid[i, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
            X_valid[i, 1, :] = mav
            X_valid[i, 2, :] = miv
        elif validation_size < i  and i < validation_size + test_size:
            # softmax 1 hot encoding 
            Y_test[i - test_size, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
            X_test[i - test_size, 1, :] = mav
            X_test[i - test_size, 2, :] = miv
        else:
            Y_train[i - train_size, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
            X_train[i - train_size, 1, :] = mav
            X_train[i - train_size, 2, :] = miv
        
    return X_train, Y_train, X_valid, Y_valid, X_test, Y_test



def build_model( vector_size):
    
    #batch_size = 512
    nb_epochs = 5
    model = Sequential()
# This fucking block will eat all of your goddamn memory 
# Sacrifice 3 chickens to improve accuracy 
    # sparse regularization 

    model.add(Conv1D(32, kernel_size=3,kernel_regularizer=regularizers.l2(0.005),
                     activation='relu', padding='same', input_shape=(vector_size,3 )))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Dropout(0.5))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Dropout(0.5))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Dropout(0.5))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=1, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=1, activation='relu', padding='same'))
    model.add(Dropout(0.25))
    model.add(Flatten())
    model.add(Dense(64, activation='tanh'))
    model.add(Dense(64, activation='tanh'))
    model.add(Dense(1, activation='sigmoid'))
    # Compile the model
    model.compile(loss='binary_crossentropy',
              optimizer=Adam(lr=0.001, decay=1e-6),
              metrics=['accuracy'])
    return model

def build_small_model(vector_size):
    model = Sequential()
    model.add(Conv1D(32, kernel_size=3,kernel_regularizer=regularizers.l2(0.005),
                     activation='relu', padding='same', input_shape=(vector_size,3 )))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Flatten())
    model.add(Dense(32, activation='relu', input_dim=200))
    model.add(Dense(1, activation='sigmoid'))
    model.compile(optimizer='rmsprop',
                  loss='binary_crossentropy',
                  metrics=['accuracy'])
    return model

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
    #model = build_model(max_tweet_length, vector_size)
    # static names 
    dataset_location = './Sentiment Analysis Dataset.csv'
    model_location = './model/'
    tokenized_corpus_name = "tokenized_tweet_corpus.dill"
    groun_truth_name  = 'ground_truth_tokenized_tweet_corpus.dill'
    model_name = 'tweet_word2vec.model'

    # Load all data
    with open(model_location + tokenized_corpus_name, 'rb') as f:
        tokenized_corpus = dill.load(f)

    with open(model_location + groun_truth_name, 'rb') as f:
        ground_truth = dill.load(f)

    # Load model and retrieve word vectors 
    word2vec = Word2Vec.load(model_location + model_name)
    X_vecs = word2vec.wv
    vector_size = 512
# generate model 
    #model = build_model(max_tweet_length, vector_size)
    # static names 
    dataset_location = './Sentiment Analysis Dataset.csv'
    model_location = './model/'
    tokenized_corpus_name = "tokenized_tweet_corpus.dill"
    groun_truth_name  = 'ground_truth_tokenized_tweet_corpus.dill'
    model_name = 'tweet_word2vec.model'

    # Load all data
    with open(model_location + tokenized_corpus_name, 'rb') as f:
        tokenized_corpus = dill.load(f)

    with open(model_location + groun_truth_name, 'rb') as f:
        ground_truth = dill.load(f)

    # Load model and retrieve word vectors 
    word2vec = Word2Vec.load(model_location + model_name)
    X_vecs = word2vec.wv
    batch_size = 64
    nb_epochs = 100
    test_size = 100000
    validation_size = 100000
    #dump_files = [X_train, Y_train, X_valid, Y_valid, X_test, Y_test ]
    # Super fucing memoery intensive 
    #print("Dataset has been created ")
    #print("DATA SHAPE: {}".format(X_train.shape))
    print("Starting data pull")
    #test_size = 100000
    #validation_size = 100000
    train_size =  len(tokenized_corpus) -  test_size - validation_size
    # pull uses around 9GBs of local memroy 
    X_train, Y_train, X_valid, Y_valid, X_test, Y_test = build_train_valid_test(tokenized_corpus, ground_truth, train_size, validation_size, test_size, X_vecs )
    # reshape 
    X_train = X_train.reshape((X_train.shape[0], X_train.shape[1] * X_train.shape[2]))
    print("Removing examples with NANs from training, testing and valid ")

    dummy_train = [~np.isnan(X_train).any(axis=1)]
    X_train = X_train[dummy_train]
    Y_train = Y_train[dummy_train]
    # Free up some space
    del dummy_train
    # shape into tensor 
    X_train = X_train.reshape((X_train.shape[0],vector_size,3))

    print("Dropping Nan rows from Validation set ")
    # flatten  feature vectors 
    X_valid = X_valid.reshape((X_valid.shape[0], X_valid.shape[1] * X_valid.shape[2]))
    dummy_valid = [~np.isnan(X_valid).any(axis=1)]

    X_valid = X_valid[dummy_valid]
    Y_valid = Y_valid[dummy_valid]
    del dummy_valid
    # Reshape valid into sensor
    X_valid =  X_valid.reshape((X_valid.shape[0], vector_size ,3))
    print("Building model with X_train.shape{}".format(X_train.shape))
    print("Building model with X_valid.shape{}".format(X_valid.shape))
    #model = build_model(vector_size)
    model = build_small_model(vector_size)
    checkpoint = ModelCheckpoint('weights.{epoch:03d}-{val_acc:.4f}.hdf5', monitor='val_acc', verbose=1, save_best_only=True, mode='auto')
    model.fit(X_train, Y_train[:,1:],
      batch_size=64,
      shuffle=True,
      epochs=nb_epochs,
      validation_data=(X_valid, Y_valid[:,1:]),
      callbacks=[EarlyStopping(min_delta=0.00025, patience=2), checkpoint])

    print("Model complete, saving")
    model.save_weights('mean_reduced_deep_nn_2_weights.h5')
    model.save("mean_reduced_complete.h5")


if __name__ == "__main__":
    main()
