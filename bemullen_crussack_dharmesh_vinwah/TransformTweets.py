# Filename: TransformTweets.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import subprocess
import DharmeshDataMechanics.CS591 as Constants
import dharmSentiment.speedy as dharmSentiment
import dharmSentiment.storyLab as dharmSentimentRender
import re

class TransformTweets(dml.Algorithm):
    contributor = Constants.CONTRIBUTOR
    key = "tweets"
    reads = [Constants.BASE_AUTH + "." + key]
    writes = [Constants.BASE_AUTH + "." + key]

    one_text = ""
    two_text = ""

    @staticmethod
    def dictifyTweet(tweetText):
        '''Generate a word dict to test.'''
        ref_text_raw = tweetText
        test_dict = dict()
        replaceStrings = ['---','--','\'\'']
        for replaceString in replaceStrings:
            ref_text_raw = ref_text_raw.replace(replaceString,' ')
        words = [x.lower() for x in re.findall(r"[\w\@\#\'\&\]\*\-\/\[\=\;]+",ref_text_raw,flags=re.UNICODE)]
        for word in words:
            if word in test_dict:
                test_dict[word] += 1
            else:
                test_dict[word] = 1        
        return test_dict

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(Constants.BASE_AUTH, Constants.BASE_AUTH)

        tweets = repo[Constants.BASE_NAME + "." + "tweets"]
        for tweet in tweets.find():
            tweets.update_one({'_id': tweet['_id']},
                {
                '$set': {
                'datetime': datetime.datetime.strptime(tweet['date'] + ' ' + tweet['time'], '%Y-%m-%d %H:%M:%S')
                }
                })

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def sentimentTest():
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(Constants.BASE_AUTH, Constants.BASE_AUTH)
        tweets = repo[Constants.BASE_NAME + "." + "tweets"]
        tList = []
        m1 = 11; m2 = m1 + 1
        normative_range = [datetime.datetime(2016, m1, 23), datetime.datetime(2016, m2, 10)]
        for tweet in tweets.find({'datetime': {'$gte': normative_range[0],
            '$lt': normative_range[1]}}):
            print("Append: " + str(tweet))
            tList.append(str(tweet['content']) + '\n')

        # mid = int((len(tList) + 1)/2)
        # print(mid)
        # firstHalf = tList[:mid]
        # firstHalf = str(firstHalf)
        # secondHalf = str(tList[mid:])
        eval_range = [datetime.datetime(2016, 8, 23), datetime.datetime(2016, 9, 10)]
        ref_dict = TransformTweets.dictifyTweet(str(tList)) #TransformTweets.dictifyTweet(firstHalf) #("../examples/data/18.01.14.txt")

        cList = []
        for tweet in tweets.find({'datetime': {'$gte': eval_range[0],
            '$lt': eval_range[1]}}):
            #print("Append: " + str(tweet['content']))
            cList.append(str(tweet['content']) + '\n')
        print("cList: " + str(cList))
        comp_dict = TransformTweets.dictifyTweet(str(cList)) #("../examples/data/21.01.14.txt")

        # this test the loading for each
        senti_dicts = [dharmSentiment.LabMT(),dharmSentiment.ANEW()]
        senti_marisas = [dharmSentiment.LabMT(datastructure="marisatrie"),
        dharmSentiment.ANEW(datastructure="marisatrie")]
        stopVal = 1.0
        for senti_dict,senti_marisa in zip(senti_dicts,senti_marisas):

            # build it out here
            ref_word_vec = senti_marisa.wordVecify(ref_dict)
            ref_word_vec_stopped = senti_marisa.stopper(ref_word_vec,stopVal=stopVal)
            comp_word_vec = senti_marisa.wordVecify(comp_dict)
            comp_word_vec_stopped = senti_marisa.stopper(comp_word_vec,stopVal=stopVal)        
            dharmSentimentRender.shiftHtml(senti_marisa.scorelist, senti_marisa.wordlist,
                ref_word_vec_stopped, comp_word_vec_stopped,
                "test-shift-{0}.html".format(senti_dict.title),corpus=senti_marisa.corpus,
                title="DharmSentiment Analysis" + senti_marisa.corpus,
                customTitle=True, ref_name="Reference Week " + str(normative_range[0].month) +
                "-" + str(normative_range[0].day) + "-" + str(normative_range[0].year) + " to "
                + str(normative_range[1].month) + "-" + str(normative_range[1].day) + "-" +
                str(normative_range[1].year)
                ,comp_name="Move-In Week: " + str(eval_range[0].month) + "-" + str(eval_range[0].day) +
                "-" + str(eval_range[0].year) + " to " + str(eval_range[1].month) + "-" +
                str(eval_range[1].day) + "-" + str(eval_range[1].year))

            # dharmSentimentRender.shiftHtml(senti_marisa.scorelist, senti_marisa.wordlist,
            #     ref_word_vec, comp_word_vec,
            #     "test-shift-titles.html".format(senti_dict.title),customTitle=True,
            #     title="Insert title here",ref_name="bananas",comp_name="apples")

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(Constants.BASE_AUTH, Constants.BASE_AUTH)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')


        this_script = doc.agent('alg:' + Constants.BASE_NAME + '#TransformTweets',\
            {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        tweets = doc.entity('dat:' + Constants.BASE_NAME + '#tweets',\
            {prov.model.PROV_LABEL:'Tweets about Boston City', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_tweets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Tweets about Boston City posted during 2016'})        
        doc.wasAssociatedWith(get_tweets, this_script)
        doc.used(get_tweets, tweets, startTime)
        doc.wasAttributedTo(tweets, this_script)
        doc.wasGeneratedBy(tweets, get_tweets, endTime)        
        
        repo.logout()
        return doc

if __name__ == "__main__":
    #TransformTweets.execute()
    TransformTweets.sentimentTest()
