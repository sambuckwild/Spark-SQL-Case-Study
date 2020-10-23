import pyspark as ps
import json
import pandas as pd 
import numpy as np 
import scipy.stats as stats
import matplotlib.pyplot as plt 
from pyspark.sql.types import *

sc = spark.sparkContext

def create_df_from_json(master, app_name, tweet_data, sample = False, sample_size = 0.2):
    Spark = ps.sql.SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()
    tweets_df = spark.read.json(tweet_data).sample(sample, sample_size)
    return tweets_df

master = "local[4]"
app_name = "/df case study"
tweet_data = './data/french_tweets.json'
#create our original tweets_df
tweets_df = create_df_from_json(master, app_name, tweet_data)

def query_results_tweets(df, ):
    pass
    #function to get SQL query results

def create_clean_df(query_results, schema):
    return spark.createDataFrame(query_results, schema)

tweet_schema = StructType( [
    StructField('Tweet_ID', IntType(), True),
    StructField('Username',StringType(),True),
    StructField('User_Country',StringType(),True),
    StructField('Tweet_Text', StringType(), True),
    StructField('Retweet_Count', FloatType(), True)] )
#create cleaned tweet dataframe
clean_tweet_df = create_clean_df(results, tweet_schema)

ht_schema = StructType( [
    StructField('Tweet_ID', IntType(), True),
    StructField('Hashtag',StringType(),True)] )
#create cleaned hashtag dataframe
ht_df = create_clean_df(results, ht_schema)