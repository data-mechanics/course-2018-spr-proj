## CS591: Urban Sentiment Analysis
## Members:  
- Kai Bernardini
- Vasily Ilin

![logo](MassGovernor_right_word_cloud.png)

# Overview Project 2:
Using tweepy, an open source python wrapper for Twitter's dev API, and an annotated tweet sentiment dataset, we
construct a fully end to end Machine Learning pipeline for predicting aggregate sentiment by region.
- There is functionality fo constructing twitter word clouds for arbitrary hashtag sentiment and specific user sentiment.
  - e.g., you can construct word clouds for a region like MA or a specific user like @realDonaldTrump
- note:  As this is predominantly exploratory,  technical short cuts were taken.

## Future Work:
- Sentiment analyzer for News API article headlines
- Correlate Twitter Sentiment with Article Sentiment
- Political Research using Twitter text mining.

## Dependencies

```python
pip install -r requirements.txt
```


# Datasets and Authentication
Before getting started, you must acquire several authentication tokens
Please modify [auth.json](https://github.com/kaidb/course-2018-spr-proj/blob/master/auth.json)
You will need the following
- Twitter Consumer API key
- Consumer Secret key
- Access Key
- Access Secret Key
- [Mapbox Access Token](https://www.mapbox.com/signin/?route-to=%22/account/access-tokens%22)

For all twitter access tokens, sign up [here](https://apps.twitter.com/)

```python
{
"consumer_key": "",
"consumer_secret": "",
"access_key": "",
"access_secret": "",
"mapbox_access_token":""
}
```
## Building the Sentiment Classifier
- execute the following to
1) download and parse all data and 2) train and validate the sentiment Classifier

```python
bash build_model.sh
```


- Optionally, to build the word2vec model

```python
bash build_word2vec.sh
```

- to run in trial mode,

```python
$ python Twitter_to_vec/get_clean_twitter_sentiment.py t
```

- Trial mode will only tokenize and stem the first few tweets for illustrative purposes, but I recommend just running the full pipeline and getting a coffee.


- The word2vec model is incredibly effective at identifying useful embeddings for profaninty, slang, and spelling associations and is not useful for word embeddings for arbitrary texts.
- Full pipeline is
```bash
Twitter_to_vec/get_clean_twitter_sentiment.py
Twitter_to_vec/build_twitter_tweet_2_vec.py
Twitter_to_vec/sentiment_model_l2_LR.py
```

- the first script will retrieve the data, save it, stem/tokenize it, and save it. This is available in trial mode
- The second one will build a word2vec model using the saved data. This is also available in trial mode
- The final one will build a Logistic Regression model on the training set and validate it. Trial mode is left out in lieu of loading a prebuilt model.


## Twitter Live stream using tweepy
- We provide several high level  scripts to allow for easy retrieving of tweets
    - Tweet streaming by Geographical Location :
    - Tweets by User
- For either usecase, you are required to create a twitter app an input the following into ```auth.json```:




## Tweet Word Embedding Examples
** Disclaimer: These associations are constructed by the tweet. word2vec model. These associations are not reflective of our views. **
** Trigger Warning: Profanity, slurs...etc **

```python
print(word2vec.wv.similar_by_word("lol"))
[('hah', 0.7501074075698853), ('lmao', 0.7403647899627686), ('hahah', 0.6293175220489502), ('hahahah', 0.4980482757091522), ('lmfao', 0.4653286337852478), ('heh', 0.46210041642189026), ('cuz', 0.4489451050758362), ('ha', 0.4350995123386383), ('jus', 0.4144180417060852), ('u', 0.41266563534736633)]
```

```python
print(word2vec.wv.similar_by_word("fuck"))
[('fuckin', 0.5401198863983154), ('shit', 0.47455620765686035), ('damn', 0.47374287247657776), ('eff', 0.4427247643470764), ('ugh', 0.42223554849624634), ('bitch', 0.41461408138275146), ('stupid', 0.39982643723487854), ('screw', 0.37740325927734375), ('wtf', 0.3753121793270111), ('freakin', 0.37031519412994385)]
```

```python
print(word2vec.wv.similar_by_word("dumb"))
[('stupid', 0.4474240839481354), ('lam', 0.44008922576904297), ('retard', 0.3861704468727112), ('smart', 0.3855532705783844), ('weird', 0.37820345163345337), ('gay', 0.3473893404006958), ('funny', 0.33632892370224), ('confus', 0.3293963372707367), ('annoy', 0.3184397220611572), ('bad', 0.2968796491622925)]
```
## Wikipedia Corpus
- The latest wikipedia dataset is located at   -https://dumps.wikimedia.org/enwiki/latest/
    - The dataset we used is at -https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        - Note that the Mirror to download is prohibitively slow. Please email me at kaidb@bu.edu and I will share the google drive containing the data (way, way faster)
        - The zipped size is ~14GBs
        - Unzipped size is ~65GBs
    - Data is then unpacked from its xml format, tokenized, and a word2vec model is constructed from it.
        - This model is too large to fit in github. For a copy, please email me and I will share the drive containing the model.
            - Useful for word embeddings for article titles (language tends to be gramatically correct)


## Optimization:
- Word2Vec using Gensim
- Gradient Descent with Momentum written from scratch :
    - See Ipython Notebook in Tweet2Vec for this
- Unfortunetly, this model proved to be too simple, and failed in the long term.
## Statistical Analysis:
- Predictive accuracy
- Sentiment Mapping
- Model Construction using Keras with Tensorflow Backend.

## Statistical Analysis
