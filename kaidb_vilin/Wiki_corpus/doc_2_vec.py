#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import os.path
import sys

from gensim.corpora.wikicorpus import WikiCorpus
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from pprint import pprint
import multiprocessing


class TaggedWikiDocument(object):
    def __init__(self, wiki):
        self.wiki = wiki
        self.wiki.metadata = True
    def __iter__(self):
        for content, (page_id, title) in self.wiki.get_texts():
            yield TaggedDocument([c.decode("utf-8") for c in content], [title])




def main():
    program = os.path.basename(sys.argv[0])
    logger = logging.getLogger(program)
 
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)
    logger.info("running %s" % ' '.join(sys.argv))
    
    print("Loading Wiki Corpus")
    wiki = WikiCorpus("enwiki-latest-pages-articles.xml.bz2")
    print("wiki loaded")
    print()
    print("Parsing Documents")
    documents = TaggedWikiDocument(wiki)
    print("Documents Parsed")
    pre = Doc2Vec(min_count=0)
    pre.scan_vocab(documents)
    for num in range(0, 20):
        print('min_count: {}, size of vocab: '.format(num), pre.scale_vocab(min_count=num, dry_run=True)['memory']['vocab']/700)

    cores = multiprocessing.cpu_count()

    models = [
        # PV-DBOW 
        Doc2Vec(dm=0, dbow_words=1, size=200, window=8, min_count=19, iter=10, workers=cores),
        # PV-DM w/average
        Doc2Vec(dm=1, dm_mean=1, size=200, window=8, min_count=19, iter =10, workers=cores),
    ]
    models[0].build_vocab(documents)
    print(str(models[0]))
    models[1].reset_from(models[0])
    print(str(models[1]))

    start = time.time()
    c = -1
    for model in models:
        print("Model  building")
        c+=1
        model.train(documents, total_examples=model.corpus_count, epochs=model.iter)
        model.save( str(time.time()) + '_tweet_doc2vec{}.model'.format(str(c)))
        print(time.time() - start)

    print(time.time() - start)


if __name__ =='__main__':
    main()
    



