## CS591 Project 
## Members:  
- Kai Bernardini
- Vasily Ilin

# Overview Project 2:
Using tweepy, an open source python wrapper for Twitter's dev API, and an annotated tweet sentiment dataset, we
construct a fully end to end ML pipeline for predicting aggregate sentiment by region. Twitter has become a data
- The long term goal is to predict reactions to current news using 

- note:  As this is predominantly exploratory,  technical short cuts were taken. 

## Technical Challenges
- Size of Datasets -- they are large
- Verry high dimensional feature space 
- Difficult to fit entire dataset into memory, batch training is a must 
- The dataset is highly non-linear 
    - Logistic Regression was pretty poor at identifying and meaningul trends 
- Insufficient computational Resources
    - (Me complaining) The new Macbooks for god knows what reason don't use a Nvidia GPU and are therefore incompatible with Tensorflow GPU integration...buyer beware
        - Training is prohibitively slow as a result  (30 minutes per epoch)
        - As a result, we could only itterate through 5 epochs 
- Bots, Bots, and more freaken bots
    - Using a super crude methidology, we determined that at least 1/10 of all of our tweets are bot generated
        - And even the true figure is likely much larger. 
        - In the future, we should likely remove these tweets from the corpus. 
    
## Future Work:
- Sentiment analyzer for News API article headlines
- Correlate Twitter Sentiment with Article Sentiment 




# Datasets

## Twitter Live stream using tweepy
- We provide several high level  scripts to allow for easy retrieving of tweets
    - Tweet streaming by Geographical Location : 
    - Tweets by User
- For either usecase, you are required to create a twitter app an input the following into ```auth.json```:

```python
{ 
    "consumer_key": "",
    "consumer_secret": "",
    "access_key": "",
    "access_secret": "",
}
```

## Twitter Pre-labled Sentiment Dataset 
- Prelabled Twitter Dataset complete with a sentiment Label 
- To retrieve, save, and tokenize the data, execute </code> $ python get_clean_twitter_sentiment.py </code>
    - to run in trial mode, </code> $ python get_clean_twitter_sentiment.py t </code>
        - Trial mode will only tokenize and stem the first few tweets for illustrative purposes
    - The model is incredibly effective at identifying useful embeddings for profaninty, slang, and spelling associations..etc and is not useful for word embeddings for arbitrary texts. 
- Full pipeline is 
```bash 
get_clean_twitter_sentiment.py
build_twitter_tweet_2_vec.py
build_clf.py
```

- the first script will retrieve the data, save it, stem/tokenize it, and save it. This is avaiable in trial mode
- The second wone will build a word2vec model using the saved data. This is also avaiable in trial mode
- The final one will build a conv network from scratch from the full dataset. Trial mode is left out in lieu of loading a prebuild mode. 
- If you do decide to from scratch, it is advised that you use a device with a GPU compatible with the tensorflow backend. 
    - there is an initial memory explosion when building the training, testing and validation set. 
    - TODO: partition all data into batches via data-stream 
    - The W2vec model is combined with randomly subsampled tokens from the corpus conditioned on 1) the word being a vector, and 2) it being reasonably common (arbitrary)
    - These are stacked into a tensor to construct the datasets. 
    - ideally, 10-20 epochs should be run with a Nvidia GPU
## Tweet Word Embedding Examples 
** Disclaimer: These asosciations are constructed by the tweet. word2vec model. These associations are not reflective of our views. **
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
##

# News API 
- We created 4 api keys and cycle through them when throttled (only 1 is required. )
```python
{
"news1": "",
"news2": "",
"news3": "",
"news4": ""
}
```
    - TODO: Aws lambda would be an awesome way to generate new keys. 
- We crawled articles from multiple national and local news sources and tried to apply the twitter sentiment classifer to them (shocker of the day, it did not transfer well)
- To remedy this, we constructed a W2vec model on the entire wikipedia corpus. 
- Unfortunetly, due to computational and timming constraints, we were unabe to apply this directly to the transformed news data. As such, the news sentiment analysis is in need of improvement. 
- 

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







# Leftover from Project 1
 ## Overview Project 1:
We compare the public employee earning report and the monthly utility bills from Analyze Boston by zip code to see if there
is a correlation between the average income in a neighborhood (we approximated it using the income of public employees as a proxy)
and how much the neighborhood pays per unit of electricity. No correlation was found.
We also retrieved the CDC binge drinking dataset by state. So far we found ten census tracts in Boston with the worst binge drinking habits.
This will likely prove useful in analyzing city life later on. For example, comparing binge drinking with the locations of colleges
or the poorest neighborhoods in Boston in order to find what binge drinking correlates more with. From the 311 dataset, there are several complaints that have reason listed as mbta. We examine geospatial relationships between the mbta transportation data, payroll information by zipcode...etc

## Transformations: 
- Payrol: Convert dollar strings to floats
- 311: Project Data to remove extraneous columns. Combine Submission time, estimated completion time  and actual completion time to compute
    - Elapsed time 
    - Estimated time till completion
    - Extra Time used to complete task 
- CDC: Various descriptive statistics, selection via boston rows...etc


## MBTA Auth. 
Please add you MBTA API_v3 key to auth.json. 

<code> {"mbta_api_key": ""}
</code>

## Generate Prov. 
In the top directory of this project, run

<code>python execute.py kaidb_vilin --trial
 </code>
This will take at 1-10 minutes depending on your connection. 
## Python Reqs:
pandas, sklearn, dml, prov, json,urlib, json, datetime, uuid