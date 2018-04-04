"""
Tweepy python script used to retrieve all tweets from a particular user
@Author: Kai Bernardini

Tweepy -- https://github.com/tweepy/tweepy
adapted from. -- (TODO: link to github)
"""
import tweepy 
# I/O read/writes
import pandas as pd
import os.path as path
import sys

#Twitter API credentials
# Do not leave values hardcoded 


two_up =  path.abspath(path.join(__file__ ,"../../.."))
auth_df = eval(open( two_up + "/auth.json").read())
print(auth_df.keys())
consumer_key = auth_df['consumer_key']
consumer_secret = auth_df['consumer_secret']
access_key = auth_df['access_key']
access_secret = auth_df['access_secret']

def get_all_tweets(screen_name, use_pandas = False):
    """Retrieve all tweets froma. particular users by their username
    - Notes: Twitter will on ly store the last 3,240 tweets from a particular 
    user using standard Dev creds. """
    
    # Authorization and initialization
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    
    #initialize a  dumb-list to hold scraped tweets
    alltweets = []  
    # Get first 200 tweets 
    new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    #save the id of the oldest tweet less one
    # This tells us where to begin our search 
    oldest = alltweets[-1].id - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print( "getting tweets before %s" % (oldest))
        
        #all subsiquent requests use the max_id param to prevent duplicates
        # We can only get at most 200 tweets per querry
        # BONUS: twitter doesn't appear to limit this. 
        # Make sure caching is enabled as to not prevent duplicate querries 
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
        print( "...{} tweets downloaded so far".format(len(alltweets)))
    if use_pandas:
        AT = [alltweets[i]._json for i in range(len(alltweets))]
        data = pd.DataFrame(AT)
        return data 

    return alltweets

def test():
    # Prepare to get mad.
    get_all_tweets("realDonaldTrump", use_pandas=True).to_csv("DJT.csv")
test()
