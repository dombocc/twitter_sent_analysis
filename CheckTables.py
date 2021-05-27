#!/usr/bin/env python
# coding: utf-8

# # Check tables to make sure they have the right number of Values


#Import Libraries
import sys
import os
import sqlite3 as lite
import datetime

#print(os.getcwd())
#print(sys.version)



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


try:
    cur.execute("create table if not exists table_checks(date_time text, raw_tweets int, deduped_tweets int, duplicates int, sentiment_total int, needs_sent int, metadata_counts int)")
except lite.OperationalError as err:
    print("insert error: %s", err)


a = cur.execute("select count(*) from tweets")
for i in a:
    print(i)
raw = i[0]


b = cur.execute("select count(*) from tweets_dedupe")
for i in b:
    print(i)
cleaned = i[0]


duplicates_count = raw-cleaned



c = cur.execute("select count(*) from sentiment")
for i in c:
    print(i)
sentiment_count = i[0]


d = cur.execute("select count(*) from tweets_dedupe where pkey not in (select pkey from sentiment)")
for i in d:
    print(i)
difference = i[0]


e = cur.execute("select count(*) from metadata")
for i in e:
    print(i)
metadata_count = i[0]


try:
    cur.execute("INSERT INTO table_checks VALUES (?,?,?,?,?,?,?)",
                (datetime.datetime.now(), raw, cleaned, duplicates_count, sentiment_count, difference, metadata_count))
    metadata_insert("Table Check has been completed")
except lite.OperationalError as err:
    print("insert error: %s", err)



con.commit()



cur.close()
con.close()




