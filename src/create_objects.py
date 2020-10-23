from clean_data import *

'''create spark instance and original dataframe'''
master = "local[4]"
app_name = "/df case study"
tweet_data = '../data/french_tweets.json'
#create spark instance
spark = create_spark(master, app_name)
#create our original tweets_df
tweets_df = create_sample_df_from_json(tweet_data)


'''Query results to make new dataframes'''
#get query results we need to make new cleaned dataframe
tweet_results = query_results_tweets(tweets_df)
#gets hashtag + tweet id results
ht_results = query_results_hashtags(tweets_df)
#gets location + tweet id results
geo_results = query_results_location(tweets_df)


'''New dataframes'''
tweet_schema = StructType( [
    StructField('Tweet_ID', StringType(), True),
    StructField('Created_At', StringType(), True),
    StructField('Username',StringType(),True),
    StructField('Tweet_Text', StringType(), True),
    StructField('Tweet_Language', StringType(), True),
    StructField('Retweet_Count', IntegerType(), True)] )
#create cleaned tweet dataframe
clean_tweet_df = create_clean_df(tweet_results, tweet_schema)

ht_schema = StructType( [
    StructField('Tweet_ID', StringType(), True),
    StructField('Hashtag',StringType(),True)] )
#create cleaned hashtag dataframe
ht_df = create_clean_df(ht_results, ht_schema)

geo_schema = StructType( [
    StructField('Tweet_ID', StringType(), True),
    StructField('Longitude',FloatType(),True),
    StructField('Lattitude',FloatType(),True)] )
#create cleaned location dataframe
geo_df = create_clean_df(geo_results, geo_schema)


'''Pandas Dataframes'''
tweet_pdf_df = pandas_df(clean_tweet_df)
ht_pd_df = pandas_df(ht_df)
geo_pd_df = pandas_df(geo_df)

