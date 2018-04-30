import json
path = "./Twitter_to_vec/"
positive_tweets = {}
with open(path+"tweets.json", "r", encoding = "utf-8") as fp:
    tweets = json.load(fp)

print(len(tweets))

negative_tweets = {}
