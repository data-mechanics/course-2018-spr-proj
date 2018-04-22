#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import dill

from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib


def main():
    keras_model = "deep_nn_weights.h5"
    model_name = 'tweet_word2vec.model'
    # static names 
    dataset_location = './Sentiment Analysis Dataset.csv'
    model_location = './model/'
    tokenized_corpus_name = "tokenized_tweet_corpus.dill"
    groun_truth_name  = 'ground_truth_tokenized_tweet_corpus.dill'
    model_name = 'tweet_word2vec.mode'
    data_loc = './Data/'
    
    csv = 'Sentiment Analysis Dataset.csv'

    my_df = pd.read_csv(csv, error_bad_lines=False)
    my_df.head()
    my_df.dropna(inplace=True)
    my_df.reset_index(drop=True,inplace=True)
    x = my_df.SentimentText
    y = my_df.Sentiment
    SEED = 6969
    x_train, x_validation_and_test, y_train, y_validation_and_test = train_test_split(x, y, test_size=.02, random_state=SEED)
    x_validation, x_test, y_validation, y_test = train_test_split(x_validation_and_test, y_validation_and_test, test_size=.5, random_state=SEED)
    del x_validation_and_test
    del y_validation_and_test
    print( "Train set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_train),
                                                                             (len(x_train[y_train == 0]) / (len(x_train)*1.))*100,
                                                                            (len(x_train[y_train == 1]) / (len(x_train)*1.))*100))
    print( "Validation set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_validation),
                                                                             (len(x_validation[y_validation == 0]) / (len(x_validation)*1.))*100,
                                                                            (len(x_validation[y_validation == 1]) / (len(x_validation)*1.))*100))
    print( "Test set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_test),
                                                                             (len(x_test[y_test == 0]) / (len(x_test)*1.))*100,
                                                                            (len(x_test[y_test == 1]) / (len(x_test)*1.))*100))
    tvec1 = TfidfVectorizer(max_features=100000,ngram_range=(1, 3))
    tvec1.fit(x_train)
    x_train_tfidf = tvec1.transform(x_train)
    x_validation_tfidf = tvec1.transform(x_validation)
    x_test_tfidf = tvec1.transform(x_test)
    print("Building Model ")
    clf = LogisticRegression(C=1, penalty='l2')
    print("Building Logistic Regression with L2 (Shrinkage) regularization")
    print("Spaese matrix represenrtation of X is used. ")
    print("Training Shape: {}".format(x_train_tfidf.shape))

    clf.fit(x_train_tfidf, y_train)
    print(clf.score(x_train_tfidf, y_train))
    print(clf.score(x_validation_tfidf, y_validation))
    print(clf.score(x_test_tfidf, y_test))

    
    np.save( data_loc + 'train_x.npy', x_train_tfidf)
    np.save( data_loc + 'train_y.npy', y_train)

    np.save( data_loc + 'valid_x.npy', x_validation_tfidf)
    np.save( data_loc + 'valid_y.npy', y_validation)

    np.save( data_loc + 'test_x.npy', x_test_tfidf )
    np.save( data_loc + 'test_y.npy', y_test)
    print("all Datasets have been saved ")

    joblib.dump(clf, model_location + 'l2_LR.pkl') 
    print("Model saved")




    with open(model_location + 'vectorizer.pk', 'wb') as fin:
        pickle.dump(tvec1, fin)
        



if __name__ == '__main__':
    main()