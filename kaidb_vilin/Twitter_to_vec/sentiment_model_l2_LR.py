#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
This is the primary file for building the logistic regression model 

"""

import pandas as pd
import numpy as np
import dill
import json
import time
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib


def main():
    start = time.time()
    # get pathing information 
    config = eval(open( '../config.json').read())
    # static names 
    print(config.keys())
    dataset_name = config["dataset name"]
    dataset_location = './{}'.format(dataset_name)
    model_location = './model/'
    tokenized_corpus_name = config["tokenized corpus name"]
    groun_truth_name  = config["ground truth labels"]
    data_loc = './Data/'
    
    csv = 'Sentiment Analysis Dataset.csv'

    my_df = pd.read_csv(csv, error_bad_lines=False)
    my_df.head()
    my_df.dropna(inplace=True)
    my_df.reset_index(drop=True,inplace=True)
    x = my_df.SentimentText
    y = my_df.Sentiment
    SEED = 6969  # for reproducability
    x_train, x_validation_and_test, y_train, y_validation_and_test = train_test_split(x, y, test_size=.02, random_state=SEED)
    x_validation, x_test, y_validation, y_test = train_test_split(x_validation_and_test, y_validation_and_test, test_size=.5, random_state=SEED)
    del x_validation_and_test
    del y_validation_and_test # free up memory 
    print( "Train set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_train),
                                                                             (len(x_train[y_train == 0]) / (len(x_train)*1.))*100,
                                                                            (len(x_train[y_train == 1]) / (len(x_train)*1.))*100))
    print( "Validation set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_validation),
                                                                             (len(x_validation[y_validation == 0]) / (len(x_validation)*1.))*100,
                                                                            (len(x_validation[y_validation == 1]) / (len(x_validation)*1.))*100))
    print( "Test set has total {0} entries with {1:.2f}% negative, {2:.2f}% positive".format(len(x_test),
                                                                             (len(x_test[y_test == 0]) / (len(x_test)*1.))*100,
                                                                            (len(x_test[y_test == 1]) / (len(x_test)*1.))*100))
    tvec1 = TfidfVectorizer(max_features=150000,ngram_range=(1, 3))
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
    print("Saving datasets ")
    time_app = str(time.time())
    np.save( data_loc + 'train_x.npy', x_train_tfidf)
    np.save( data_loc + 'train_y.npy', y_train)

    np.save( data_loc + 'valid_x.npy', x_validation_tfidf)
    np.save( data_loc + 'valid_y.npy', y_validation)

    np.save( data_loc + 'test_x.npy', x_test_tfidf )
    np.save( data_loc + 'test_y.npy', y_test)
    print("all Datasets have been saved ")

    print("Saving model .......")
    model_name = 'l2_LR{}.pkl'.format(time_app)
    config["Model name"] = model_name
    joblib.dump(clf, model_location + model_name) 
    print("Model saved under {}".format(model_location + model_name))
    print()
    print("Saving Vectorizer")
    vectorizer_name = 'vectorizer{}.pk'.format(time_app)
    config["Vectorizer name"] = vectorizer_name
    with open(model_location + vectorizer_name,'wb') as f:
        pickle.dump(tvec1, fin)
    print("Vectorizer saved under {}".format(model_location + vectorizer_name))
    print("Updated config.json")
    with open('../config.json', 'w') as f:
        f.write(json.dumps(config))

    print("Finished in {}".format(time.time() - start))
if __name__ == '__main__':
    main()