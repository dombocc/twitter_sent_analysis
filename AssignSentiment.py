#!/usr/bin/env python
# coding: utf-8

# # Classify sentiment of tweets in database

# In[1]:


#Import Libraries
import sys
import os
import sqlite3 as lite

from pandas import Series, DataFrame
import pandas as pd
import numpy as np
import nltk
import re
from nltk.stem import WordNetLemmatizer
from sklearn.svm import LinearSVC
from sklearn.metrics import classification_report
import sklearn.metrics
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LogisticRegression
import json
import pickle
import datetime


pd.set_option('display.max_rows',1000)
pd.set_option('display.max_columns',1000)


#print(os.getcwd())
#print(sys.version)


#Create a Connector to Sqlite
con = lite.connect(r'tweets.db')  

cur = con.cursor()


def metadata_insert(message):
    try:
        cur.execute("INSERT INTO metadata VALUES (?,?)",
                    (datetime.datetime.now(),message))
    except lite.OperationalError as err:
        print("insert error: %s", err)


df = pd.read_sql_query("""
                SELECT * 
                FROM tweets_dedupe a
                where a.pkey not in (select pkey from sentiment)
                ;""", con)
df.shape[0]


df['lemm_tweet'] = [''.join([WordNetLemmatizer().lemmatize(re.sub('[^A-Za-z]', ' ', line)) for line in lists]).strip() for lists in df['tweet']] 


#build training corpus
traindf = pd.read_json("Training_Data.json")



traindf['tweet_clean_string'] = [' , '.join(z).strip() for z in traindf['tweet']]  
traindf['tweet_string'] = [' '.join([WordNetLemmatizer().lemmatize(re.sub('[^A-Za-z]', ' ', line)) for line in lists]).strip() for lists in traindf['tweet']]       
corpustr = traindf['tweet_string']
vectorizertr = TfidfVectorizer(stop_words='english',
                             ngram_range = ( 1 , 1 ),analyzer="word", 
                             max_df = .57 , binary=False , token_pattern=r'\w+' , sublinear_tf=False)
tfidftr=vectorizertr.fit_transform(corpustr)



corpustweets = df['lemm_tweet']
vectorizertweets = TfidfVectorizer(stop_words='english')
tfidftweets=vectorizertr.transform(corpustweets)


tweets_to_classify = tfidftweets


#Load Model
filename = "sentiment_model.sav"
loaded_model = pickle.load(open(filename, 'rb'))


loaded_model


predictions = loaded_model.predict(tweets_to_classify)
df['sentiment'] = predictions
# testdf = testdf.sort_values('id' , ascending=True)


df_sent = df[['pkey','sentiment']]


df_sent.to_sql('sentiment',con=con, if_exists='append', index=False)


con.commit()


rows = df_sent.shape[0]
rows
metadata_insert(str(rows) + " new rows assigned a sentiment")

con.commit()

cur.close()
con.close()






