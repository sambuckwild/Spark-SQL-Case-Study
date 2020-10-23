import pyspark as ps
import json
import pandas as pd 
import numpy as np 
import scipy.stats as stats
import matplotlib.pyplot as plt 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re
import nltk
import string
from nltk.corpus import stopwords

'''Create spark instance'''
def create_spark(master, app_name):
    spark = ps.sql.SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()
    return spark
'''create spark instance'''
master = "local[4]"
app_name = "/df case study"
tweet_data = '../data/french_tweets.json'
#create spark instance
spark = create_spark(master, app_name)

'''Create dataframe'''
def create_sample_df_from_json(tweet_data, sample = False, sample_size = 0.2):
    tweets_df = spark.read.json(tweet_data).sample(sample, sample_size)
    return tweets_df

def create_df_from_json(tweet_data):
    tweets_df = spark.read.json(tweet_data)
    return tweets_df


'''Get various query results to make df's we can merge and compare'''
def query_results_tweets(df):
    df.createOrReplaceTempView('tweet_df')
    result = spark.sql("""SELECT id,
    created_at,
    user.name,
    text,
    lang,
    retweet_count
    FROM tweet_df LATERAL VIEW explode(entities.hashtags.text) AS hashtag
    WHERE hashtag is not null
    LIMIT 10
    """).collect()
    return result

def query_results_hashtags(df):
    df.createOrReplaceTempView('ht_df')
    result = spark.sql("""SELECT id, hashtag
    FROM ht_df LATERAL VIEW explode(entities.hashtags.text) AS hashtag
    WHERE hashtag is not null
    """).collect()
    return result

def query_results_location(df):
    df.createOrReplaceTempView('geo_df')
    result = spark.sql("""SELECT id,
    geo.coordinates[0] as long, geo.coordinates[1] as lat
    FROM geo_df LATERAL VIEW explode(entities.hashtags.text) AS hashtag
    WHERE hashtag is not null
    """).collect()
    return result


'''create cleaned dataframes to work with'''
def create_clean_df(query_results, schema):
    return spark.createDataFrame(query_results, schema)                 


'''Make into Pandas Dataframe'''
def pandas_df(df):
    return df.toPandas()

'''cleans text from symbols and emojis etc'''
 def text_all_cleaned(x):
    x = x.lower()
    x = ' '.join([word for word in x.split(' ') if word not in stop_words])
    x = x.encode('ascii', 'ignore').decode()
    x = re.sub(r'https*\S+', ' ', x)
    x = re.sub(r'@\S+', ' ', x)
    x = re.sub(r'#\S+', ' ', x)
    x = re.sub(r'\'\w+', '', x)
    x = re.sub('[%s]' % re.escape(string.punctuation), ' ', x)
    x = re.sub(r'\w*\d+\w*', '', x)
    x= re.sub(r'\s{2,}', ' ', x)
    return x