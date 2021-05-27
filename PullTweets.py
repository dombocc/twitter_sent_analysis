#!/usr/bin/env python
# coding: utf-8

# # Twitter Sentiment Analysis of Certain Companies


#Import Libraries
import sys
import os
import numpy as np
import pandas as pd
import tweepy
import sqlite3 as lite
import requests
from requests_oauthlib import OAuth1
import os
import datetime

#pd.set_option('display.max_rows',1000)
#pd.set_option('display.max_columns',1000)


#print(os.getcwd())
#print(sys.version)



#Twitter Keys
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



import datetime
import html
import unidecode
import re

def clean_dates(d):
    date = d.strftime("%Y-%m-%d %H:%M:%S")
    return date


def decode2(s):
  t = unidecode.unidecode(s)
  if (t == '[?]'): 
    return '_'
  else:
    return(t)

def decodeStr(s):
    t =""
    for x in s:
      t = t+decode2(x)
    return t


def clean_string(s):
    s = html.unescape(s)
    s = decodeStr(s)  #convert foreign language to bad-english
    skips = re.compile(r"[^\x00-\x7F]")  #get rid of anything non-ascii
    s = re.sub(skips," ",s)
    s = re.sub('  ',' ',s)
    s = s.replace('\t',' ')
    s = s.replace('\n',' ')
    s = s.replace("'","")  #get rid of single quotes
    s = s.replace('"',' ')  #get rid of double quotes
    s = s.replace('  ',' ') #get rid of extra spaces
    return(s.strip())

def cleaned_tweet(s):
    s=clean_string(s)
    t=clean_string(s)
    while(s!=t):
        s=t
        t=clean_string(s)
    return(s)

def get_max_id(key):
    query = "Select max(tweetID) from tweets where keyword = '" + key + "'"
    id_list = cur.execute(query)
    for i in id_list:
        max_id = i[0]
    return(max_id)
    


# # SQL Connector

#Create a Connector to Sqlite
con = lite.connect(r'tweets.db')  

cur = con.cursor()

#Insert message into metadata
def metadata_insert(message):
    try:
        cur.execute("INSERT INTO metadata VALUES (?,?)",
                    (datetime.datetime.now(),message))
    except lite.OperationalError as err:
        print("insert error: %s", err)


#Function to pull tweets and insert into table
def pull_tweets(searchword,quantity_tweets):
    a = 0
    max_id = get_max_id(searchword)   
#     max_id = '0'
    
    search = searchword + " -filter:retweets"
    tweets = tweepy.Cursor(api.search, q= search, lang='en', tweet_mode='extended').items(quantity_tweets)
    
    for tweet in tweets:
        date = clean_dates(tweet.created_at)
        tweetid = tweet.id_str
        tweet_text = cleaned_tweet(tweet.full_text)
        handle = tweet.user.screen_name
        pri_key = tweetid+'_'+searchword
#         print(date,tweetid,tweet_text,handle)
        if tweetid > max_id:
            try:
                cur.execute("INSERT INTO tweets VALUES (?,?,?,?,?,?)",
                            (date, searchword, tweetid, tweet_text, handle, pri_key))
            except lite.OperationalError as err:
                print("insert error: %s", err)
                break
            a += 1
                
    con.commit()
    print(searchword, a, 'rows committed in total')
    metadata_insert(searchword + ' ' + str(a) + ' ' + 'rows committed in total')
    
    



pull_tweets('comcast',100)
pull_tweets('verizon',100)
pull_tweets('amazon',100)
pull_tweets('tesla',100)
pull_tweets('linux',100)
pull_tweets('apple',100)
pull_tweets('nintendo',100)
pull_tweets('sony',100)
pull_tweets('xbox',100)


x=cur.execute("select count(*) from tweets_dedupe")
for i in x:
    print(i)
a = i[0]

# Create a clean table with no duplicates
#cur.execute("drop table if exists tweets_dedupe;")
#cur.execute('create Table tweets_dedupe (date TEXT, keyword TEXT, tweetid TEXT, tweet TEXT, handle TEXT, pkey TEXT);')
cur.execute("""
        insert into tweets_dedupe(date, keyword, tweetid, tweet, handle, pkey)
            select distinct date, keyword, tweetid, tweet, handle, pkey
            from tweets
            where pkey not in ( select pkey from tweets_dedupe )
            ;
        """
        )
con.commit()

x=cur.execute("select count(*) from tweets_dedupe")
for i in x:
    print(i)
b = i[0]
        
c = b-a     
        
metadata_insert(str(c)+" tweets have been added to the tweets_dedupe table")


con.commit()

cur.close()
con.close()
