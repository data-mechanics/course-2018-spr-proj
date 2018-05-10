#!/bin/bash

# this is the series of executions used to build a Logistic regression model for Classification
# Execution should be with python 3


echo "Downloading Labled Twitter Sentiment Data"
echo "All data will be stemmed, and stored as a pickle object"


python  get_clean_twitter_sentiment.py
echo "Building the Model "
python sentiment_model_l2_LR.py
echo "DONE"
