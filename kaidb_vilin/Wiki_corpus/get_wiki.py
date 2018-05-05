import logging
import os.path
import sys
from gensim.corpora import WikiCorpus
 
def main():
    """
    The latest version of this can be found at 
    Python script to download a corpus of wikepedia articles.
    All punctuation is removed
    """

    ## Construct logger for article crawl 

    # Grab the name of the python program executing 
    print("Starting WIKI Clean")
    program_name = os.path.basename(sys.argv[0])
    # Log all outputs to std-out from this prigram
    logger = logging.getLogger(program_name)
    # Every so often, log the date time object, the opperation, and a description 
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    logger.info("running %s" % ' '.join(sys.argv))
    # check and process input arguments
    if len(sys.argv) != 3:
        print("ARGUMENT ERROR-- usage is 'python get_wiki.py enwiki.xxx.xml.bz2 wiki.en.text'")
        sys.exit(1)
    inp, outp = sys.argv[1:3]
    space = " "
    i = 0
    # Binary Mode for extra speed 
    output = open(outp, 'wb')
    # lemmatize is set to false to speed up crawl 
    wiki = WikiCorpus(inp, lemmatize=False, dictionary={})
    for text_obj in wiki.get_texts():
        # write all text to file 
        output.write( bytes(' '.join(text_obj) + '\n'
            , 'utf-8'))
        # new text obj encountered
        i = i + 1
        if (i % 10000 == 0):
            # log every 10,000  text objects 
            logger.info("Saved " + str(i) + " articles")
            # DEBUG: 
            
    output.close()
    logger.info("Crawl Completed  with  {} articles".format(i))


if __name__ == '__main__':
    main()