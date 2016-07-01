import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import csv
import math
import re

from itertools import product
import gensim, logging
from geopy.distance import vincenty
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
#import twokenize

try:
   import cPickle as pickle
except:
   import pickle


### Switch to read by line in order to get rid off broken entries quicker
def read_file(src_file):
    print("Read File")
    tweets = []

    csv.register_dialect(
        'mydialect',
        delimiter = ',',
        quotechar = '"',
        doublequote = True,
        skipinitialspace = True,
        lineterminator = '\n',
        quoting = csv.QUOTE_MINIMAL)
    

    with open(src_file, mode='r', newline='', encoding="utf-8") as f:
        reader = csv.reader(f, dialect='mydialect')
        tweets = list(reader)

    print("Reading file: " + src_file + " completed.")
    return tweets
   

#tokenise using nltk.tokenize word_tokenize
def nltk_tokenise(tweet):
    tokens = word_tokenize(tweet)
    tokenised = []
    for token in tokens:
        tokenised.append(token)
    return tokenised

#replace URLs with "URLLINK"
def replaceURLs(tweet):
    return re.sub(r"http\S+", " ", tweet)

#replace some chars that we dont want to have
def replaceChars(tweet):
    tweet = re.sub(r"\@.+\s", " ", tweet)
    result = re.sub(r"[^a-z]", " ", tweet)
    return re.sub(' +',' ', result)

#combine all the previous functions
def clean_tweet(tweet):
    
    tweet = replaceURLs(tweet)
    tweet = tweet.lower()
    tweet = replaceChars(tweet)
    tweet = nltk_tokenise(tweet)
    #do some stemming
    #porter_stemmer = PorterStemmer()
    #wordnet_lemmatizer = WordNetLemmatizer()

    #stemmed = []
    #for word in tweet: 
    #    stemmed.append(porter_stemmer.stem(word))
    #tweet = stemmed
    
    stops = set(stopwords.words("english"))
    without_stop_words = [w for w in tweet if not w in stops and len(w) > 2] 

    return (without_stop_words)

def getIDRepresentation(tweetsmeta, maxLen):
    word_to_idx = {}

    #idx = 3
    tweet_idx = []
    #run through 2dlist
    for tweet in tweetsmeta:
        #id_list = []
        tweet = clean_tweet(tweet)
        #for token in tweet:
        #     if token != "":
        #         if token in word_to_idx:
        #             id_list.append(word_to_idx[token])
        #         else:
        #             word_to_idx[token] = idx
        #             id_list.append(idx)
        #             #print("word " + token + " added using ID" + str(idx) )
        #             idx += 1    
        # padding = (maxLen-len(id_list)) 
        # id_list = [0]*(padding//2)+ id_list + [0]*(padding//2)
        # if padding %2 == 1:
        #     id_list += [0]
        #print(id_list)
        tweet_idx.append(tweet)
        #tweet_idx.append(id_list)

    return(tweet_idx, word_to_idx)
    #return(np.array(tweet_idx, dtype="int32"), word_to_idx)

def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v) 
            print(line, file=input_file)

def main():
    
    directory = "data/crawled-tweets/"
    filename = "master.csv"
    src_file = directory+filename

    tweetsmeta = read_file(src_file)

    #users = np.array()
    
    #flag = np.array([], dtype="int8")
    maxLen = 0

    longitude = []
    latitude = []
    users = []
    nonBrokenTweets = []
    timestamp = []
    omittedTweeets = 0
    numForbiddenKeywords= 0
    numTweetsProcessed = 0
    knockoutwords = {"mph", "watt", "ktt", "watts", "kts", "hiring", "bestbroadband", "barometer"}
    

    print("Remove broken and spam tweets")
    for row in tweetsmeta:
        if len(row) == 8:
            cT = clean_tweet(row[2])
            scT = set(cT)
            
            if len(scT) > 0: #andnot scT.intersection(knockoutwords) :
                
                nonBrokenTweets.append(cT)
                longitude.append(row[3])
                latitude.append(row[4])
                users.append(row[0])
                timestamp.append(row[1])
                # rwt_count.append(row[5])
                # fac_countappend(row[6])
                # flag.append(row[7])
                clen = 0
            
            
                clen = len(clean_tweet(row[2]))
                if clen > maxLen:
                    maxLen = clen
            else:
                numForbiddenKeywords += 1
                
                if numForbiddenKeywords % 1000 == 0:
                    print(str(numForbiddenKeywords) + " bot sentences omitted.")
        else:
            omittedTweeets += 1
            if omittedTweeets % 1000 == 0:
                print("There were " + str(omittedTweeets) + " omitted tweets.")

        numTweetsProcessed += 1

        if numTweetsProcessed % 100000 == 0:
            print(str(numTweetsProcessed) + " Tweets were processed.")
    
    print("The longest tweet has this many words: " + str(maxLen))
    
    nplong = np.array(longitude, dtype="float64")
    nplat = np.array(latitude, dtype="float64")
    npuser = np.array(users, dtype="int64")
    
    #remove insignificant part of timestamp
    nptimestamp = np.array(timestamp, dtype="int64") // 100000

    #tweetsinIDs, word_to_idx = getIDRepresentation(nonBrokenTweets, maxLen)

    #print(tweetsinIDs)

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    
    #size of wordembeddings
    dimensions = 100

    print("launch word2vec")
    model = gensim.models.Word2Vec(nonBrokenTweets, min_count=1, sg=0, size=dimensions, workers=4)#, negative=-5)
    model.save("worldwide.model")
    print("Pooling for each tweet")

    maxpooledTweets = np.zeros((len(nonBrokenTweets), dimensions), dtype="float64")
    #model['computer']

    #ids = [x for x in range(0,len(nonBrokenTweets))]

    for i, tweet in enumerate(nonBrokenTweets):
        itera = 0 
        for word in tweet:
            if word in model: 
                      
                #for k, dimen in enumerate(model[word]):
                    #if dimen > maxpooledTweets[i, k]:
                        #print(dimen)
                    #    maxpooledTweets[i, k] = dimen
                    #avg pooling
                itera += 1
                maxpooledTweets[i, :] += model[word]
        maxpooledTweets[i,:] = maxpooledTweets[i,:] / itera        


    #print(maxpooledTweets[0, :])
    print("Concateneate features and save them")
    features = np.concatenate((nplong[:, None], nplat[:, None], npuser[:, None], nptimestamp[:, None], maxpooledTweets), axis=1)

    #print(features)
    #write original tweets with ids to pickle to describe clusters later
    #pickle.dump(ids, "ids.pick"]
    #pickle.dump(tweetsinIDs, "tweets_in_text.pick")
    features.dump("data/features.pick")
    pickle.dump(nonBrokenTweets, open("data/pp_tweets.pick", "wb" ), 2)

    #print("write report")
    #write_report(word_to_idx, directory + "data/word2idx.dict")

if __name__ == "__main__":
    main()
