#!/bin/bash

# this is the series of executions used to build a Logistic regression model for Classification
# Execution should be with python 3


echo "Downloading Labled Twitter Sentiment Data"
echo "All data will be stemmed, and stored as a pickle object"

python  Twitter_to_vec/get_clean_twitter_sentiment.py
echo "Building the Model "
python Twitter_to_vec/sentiment_model_l2_LR.py
echo "DONE"