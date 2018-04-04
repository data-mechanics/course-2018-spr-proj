
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




def build_train_valid_test(tokenized_corpus, ground_truth, train_size, validation_size, test_size, X_vecs , max_tweet_length):
    # Generate random indexes

    vector_size = 512
    np.random.seed(6969)
    indexes = set(np.random.choice(len(tokenized_corpus), train_size + test_size, replace=False))

    X_train = np.zeros((train_size, max_tweet_length, vector_size), dtype=K.floatx())
    Y_train = np.zeros((train_size, 2), dtype=np.int32)

    X_valid = np.zeros((validation_size, max_tweet_length, vector_size), dtype=K.floatx())
    Y_valid = np.zeros((validation_size, 2), dtype=np.int32)


    X_test = np.zeros((test_size, max_tweet_length, vector_size), dtype=K.floatx())
    Y_test = np.zeros((test_size, 2), dtype=np.int32)


    # Really, really lazy way of doing this. 
    # TODO: Do this in a not so lazy way 

    # Validation is first 10,000 indicies
    # Test is the second 10,000 indicies
    # Train is the rest 
    for i, index in enumerate(indexes):
        for t, token in enumerate(tokenized_corpus[index]):
            if t >= max_tweet_length:
                break
            # Ger X vals
            if token not in X_vecs:
                continue
            # add vector to to validation set 
            if i < validation_size:
                X_valid[i, t, :] = X_vecs[token]
            # add vector to test set 
            elif  validation_size < i  and i < validation_size + test_size :
                X_test[i - test_size, t, :] = X_vecs[token]
            # add to train set 
            else: 
                X_train[i - train_size, t, :] = X_vecs[token]
        # 1 hot encoding for y balus      
        if i < validation_size :
            # softmax 1 hot encoding 
            Y_valid[i, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
        elif validation_size < i  and i < validation_size + test_size:
            # softmax 1 hot encoding 
            Y_test[i - test_size, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
        else:
            Y_train[i - train_size, :] = [1.0, 0.0] if ground_truth[index] == 0 else [0.0, 1.0]
    return X_train, Y_train, X_valid, Y_valid, X_test, Y_test



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


def main():
    max_tweet_length = 30
    vector_size = 512
    # generate model 
    model = build_model(max_tweet_length, vector_size)
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
    nb_epochs = 5
    test_size = 100000
    validation_size = 100000
    train_size =  len(tokenized_corpus) -  test_size - validation_size
    print("Train Size:{}, Validation Size:{}, Test Size:{}".format(train_size, validation_size, test_size))
    X_train, Y_train, X_valid, Y_valid, X_test, Y_test = build_train_valid_test(tokenized_corpus, ground_truth, train_size, validation_size, test_size, X_vecs, max_tweet_length )
    #dump_files = [X_train, Y_train, X_valid, Y_valid, X_test, Y_test ]
    # Super fucing memoery intensive 
    print("Dataset has been created ")
    print("DATA SHAPE: {}".format(X_train.shape))
    model.fit(X_train, Y_train,
          batch_size=batch_size,
          shuffle=True,
          epochs=nb_epochs,
          validation_data=(X_test, Y_test),
          callbacks=[EarlyStopping(min_delta=0.00025, patience=2)])
    print("Model complete, saving")
    model.save_weights('deep_nn_weights.h5')


if __name__ == "__main__":
    main()
