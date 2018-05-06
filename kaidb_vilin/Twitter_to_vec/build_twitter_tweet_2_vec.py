# Tweet to vec model builder 
# Data Source http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/
import dill
import multiprocessing
import numpy as np

from gensim.models.word2vec import Word2Vec
import time
import sys
import json


def load_corpus(model_location, corp_filename):
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



def main(trial = False):
    # we assume that the model data has already been downloaed 
    # and that the files have been stemmed
    # If not, please run get_clean_twitter_sentiment.py
    start = time.time()
    config = eval(open( '../config.json').read())
    model_location = './model/'
    tokenized_corpus_name = config["tokenized corpus name"]
    print("Loading Data....")
    tokenized_tweet_corpus = load_corpus( model_location, tokenized_corpus_name)
    print("Data loaded")
    print()
    if trial:
        print("Running in trial mode with truncated corpus")
        tokenized_tweet_corpus = tokenized_tweet_corpus[:100]

    print("Building Word2Vec Model....")
    # Train the model
    # w2vec word embeding for tweet corpus
    # TODO: enhance with wikepedia dataset
    # Maybe consider using H2o-- this is slow as fuck 
    # IF you need something to do while you wait...illicit substances optional
    # https://www.youtube.com/watch?v=pCpLWbHVNhk
    print()
    word2vec = Word2Vec(sentences=tokenized_tweet_corpus,
                    # vector size--max size of a tweet
                    size=1000, 
                    # Window size
                    window=10, 
                    negative=20,
                    iter=75,# I am impatient, and don't want to run it for too long 
                    seed=6969,
                    # heiarch
                    #s=1,
                     #negative=0,
                    # paralellize this or it will take legit forever 
                    workers=multiprocessing.cpu_count())
    elapsed_time = time.time() - start
    print("model completed  in {}".format( elapsed_time))
    tweet2vec_model_name = str(time.time()) + '_tweet_word2vec.model'
    config["tweetToVec model name"] = tweet2vec_model_name

    # just to prevent overwriting a new one 
    word2vec.save(model_location +  )
    print("Model succsesfully saved")
    print("Updating config file")
    with open('../config.json', 'w') as f:
        f.write(json.dumps(config))
if __name__ == "__main__":
    if len(sys.argv) ==2:
        if sys.argv[1] == 't':
            print("Running in Trial Mode: Truncating stemming ")
            main(True)
        else:
            print("Invalid Argument. To run in trial mode, please use '$ python {} t'".format(sys.argv[0]))
            sys.exit(1)
    elif len(sys.argv) >2:
        print("too many arguments. ")
        print("To run in trial mode, please use '$ python {} t'".format(sys.argv[0]))
        sys.exit(1)

    else:
        main()


