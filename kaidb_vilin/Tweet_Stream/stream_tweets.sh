#!/bin/bash
# Setup for Crawl Tweets

if [ ! -d "./TWEETS" ]; then
  mkdir ./TWEETS
fi
if [  -d "./TWEETS" ]; then
  echo "TWEET Directory exists, and all tweets will be stored there"
fi

if [  -f ./log.txt ]; then
    mv log.txt log.1.txt
fi

python crawl_tweets.py > log.txt