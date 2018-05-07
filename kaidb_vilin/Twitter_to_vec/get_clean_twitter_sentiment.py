# Download zip file and extract to this directory 
# Located at http://thinknook.com/wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip

import dill
import multiprocessing
import numpy as np
from gensim.models.word2vec import Word2Vec
import re
import time
from nltk.stem.lancaster import LancasterStemmer
from nltk.tokenize import RegexpTokenizer
import sys
import urllib.request
import gzip
import json 

import requests, zipfile, io


def download_zip(zip_file_url):
    print("Downloading the zip file at {}".format(zip_file_url))
    r = requests.get(zip_file_url)
    print("Extracting contents to local directory")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()


def remove_urls (vTEXT):
    vTEXT = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', vTEXT, flags=re.MULTILINE)
    return(vTEXT)



def build_corpus(dataset_location):
    """
    Parse the Twitter Sentument analysisßßß dataset.

    Arguments:
    dataset_location -- local path to the dataset. 
    It must be organized with columns 'ItemID Sentiment SentimentSource SentimentText'
    
    Returns:
    corpus -- a python list of text with each index representing the text from a tweet
    ground_truth -- ground truth sentiment labels (pre-labled)
    """
    # collection of text
    corpus = []
    # Sentiment Label
    ground_truth = []
    punctuation = re.compile(r'[><%^*-.?!,":;()|0-9]')

    # Progress
    count = 0
    start = time.time()
    with open(dataset_location, 'r', encoding='utf-8') as ds:
        print("Opening dataset")
        """
        Format of the dataset: ItemID Sentiment SentimentSource SentimentText
        """
        print("Dataset loaded. Enumeration begining")
        for i, line in enumerate(ds):
            
            count +=1
            if count%10000 ==0:
                # Logging 
                print("{} tweets parsed so far in {}".format(count, time.time() - start))
            if i == 0:
                # No data is contained in header 
                continue
            # This contains  is the ground truth label  as well as the text
            y_x_vector = line.strip().split(',')
            
            # Sentiment MAP: (0 = Negative, 1 = Positive)
            ground_truth.append(int(y_x_vector[1].strip()))
            
            # Retrieve the tweet data
            tweet = y_x_vector[3].strip()
            clean_tweet = punctuation.sub("", tweet)
            # remove urls 
            clean_tweet = remove_urls(clean_tweet)
                    
            corpus.append(clean_tweet.strip().lower())
            
    print('Corpus size: {}'.format(len(corpus)))
    print("Finished in {}".format(time.time() - start))
    return corpus, ground_truth


def tokenize_corpus(corpus):
    """
    Tokenizes a text corpus

    Arguments: 
    corpus -- python list of lists, where each entry in the list is string containing a tweet 
    """
    # Stem, tokenize, and prep for input 
    tkr = RegexpTokenizer('[a-zA-Z0-9@]+')
    stemmer = LancasterStemmer() # crap but fast 
    # Will take a few minutes
    # TODO/Future work: Break into multiple pieces and paralellize
    # ~2 seconds per 10,000 entires. 
    # with 1578627, it takes ~ 5 minutes on a macbook pro. 
    print("Go get a Coffee-- this will take ~ 5 minutes to stem/tokenize all inputs")
    ##TODO: paralellize stemming 
    tokenized_corpus = []
    start = time.time()
    token_count = 0
    for i, tweet in enumerate(corpus):
        token_count +=1
        if token_count % 10000 ==0:
            print("{} tweets tokenized this far in {}".format(token_count, time.time() - start))
        tokens = [stemmer.stem(t) for t in tkr.tokenize(tweet) if not t.startswith('@')]
        tokenized_corpus.append(tokens)
    print("Completed in {}".format(time.time()  - start))
    return tokenized_corpus


def save_corpus(corp, model_location, corp_filename):
    """
    Saves the twitter corpus  for later use

    Arguments: 
    corp -- list of lists containing tweets
    corp_filename -- save name for the corp
    """
    # Save corpus as a python object. 
    # Takes a long ass time, as the object is pretty darn large 
    with open(model_location + corp_filename , 'wb') as f:
        dill.dump(corp, f)


def load_corpus(corp_filename):
    """
    Loads the corpus dil object

    Arguments:
    corp_filename -- name of the file contained in the path ./model_location/

    Returns:
    corp -- lists of lists containing tweets 
    """
    with open(model_location + corp_filename, 'rb') as f:
        corp = dill.load(f)
    return corp



def main(trial=False):
    # location of twitter data
    dataset_url = "http://thinknook.com/wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip"
    # local path to name 
    dataset_location = './Sentiment Analysis Dataset.csv'
    # where to save the model
    model_location = './model/'
    # tokenized filename 
    corp_filename = 'tokenized_tweet_corpus.dill'
    gt_filename = 'ground_truth_tokenized_tweet_corpus.dill'
    # For reproducability, and being immature AF
    np.random.seed(6969)

    # Start by retrieving the data
    download_zip(dataset_url) 
    corpus, ground_truth = build_corpus(dataset_location)
    # for quick debug 
    if trial:
        # do a truncated write
        corpus = corpus[:1000]
        ground_truth = ground_truth[:1000]
    t_corp = tokenize_corpus(corpus)
    print("Writting Corpus to {}".format(corp_filename))
    save_corpus(t_corp, model_location, corp_filename)
    print("Writting Synchronous Lables to {}".format(gt_filename))
    save_corpus(ground_truth, model_location, gt_filename)


    

if __name__ == "__main__":
    if len(sys.argv) ==2:
        if sys.argv[1] == 't':
            print("Running in Trial Mode with Truncated stemming ")
            main(True)
        else:
            print("Invalid Argument. To run in trial mode, please use '$ python get_clean_twitter_sentiment.py t'")
            sys.exit(1)
    else:
        main()








