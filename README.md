# Spark Case Study: Politics Makes Strange Bedfellows

- [Spark Case Study: Politics Makes Strange Bedfellows](#spark-case-study-politics-makes-strange-bedfellows)
  - [Introduction](#introduction)
    - [The Dataset](#the-dataset)
    - [An Example Data.](#an-example-data)
  - [Your Tasks](#your-tasks)
  - [Hints](#hints)
    - [1. Non-UTF Characters](#1-non-utf-characters)
    - [2. Messy Data](#2-messy-data)

This is a comprehensive case study. You may want to review what you have learned so far and come up with something unique and eye-opening! **Your task is to leverage your understanding of Spark, visualization, feature engineering and relevant statistics knowledge to explore the dataset and answer the question you defined.**

## Introduction
In 2017, Emmanuel Macron and Marine Le Pen were the final two candidates in the French Presidential Election.  The two candidates had drastically different approaches to governing, and as such, the election was a major topic of discussion on Twitter. You are going to use a dataset of **216,912** tweets to study it.

### The Dataset
<a href="https://s3.us-east-2.amazonaws.com/jgartner-test-data/twitter/zippedData.zip">The data</a> you are provided a line delimited json file (746 MB) of tweets from France during that time period.
> Hint: Such a line delimited json file is also called a [`jsonl`](http://jsonlines.org/) file, which means each line of it is a string which can be converted into a json object. For example, the following code snippet demonstrates how you can read such a jsonl file into a list of json objects.

```python
import json

with open('path/to/file.jsonl', 'r') as json_file:
    json_list = list(json_file)
```
### An Example Data.

Below is a dictionary created from the second line of the jsonl file. You may look up the meaning of each field from official Twitter [doc](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview/tweet-object). Some of them may change in the past years but shouldn't be much.

```json
{
   "created_at":"Wed Apr 26 13:30:45 +0000 2017",
   "id":857225437088555009,
   "id_str":"857225437088555009",
   "text":"@julesbl99 travailles au lieu de raconter ta vie",
   "display_text_range":[
      11,
      48
   ],
   "source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e",
   "truncated":false,
   "in_reply_to_status_id":857220945982967808,
   "in_reply_to_status_id_str":"857220945982967808",
   "in_reply_to_user_id":787617474820841472,
   "in_reply_to_user_id_str":"787617474820841472",
   "in_reply_to_screen_name":"julesbl99",
   "user":{
      "id":2586505687,
      "id_str":"2586505687",
      "name":"Cerpyth",
      "screen_name":"Audran_6",
      "location":null,
      "url":null,
      "description":"Les gens depensent dl'argent qu'ils ne gagnent pas pour acheter ds choses dont ils nont pas besoin pr impressionner des gens qu'ils n'aiment pas. snap: audran_6",
      "protected":false,
      "verified":false,
      "followers_count":173,
      "friends_count":134,
      "listed_count":1,
      "favourites_count":1015,
      "statuses_count":2922,
      "created_at":"Tue Jun 24 23:16:43 +0000 2014",
      "utc_offset":null,
      "time_zone":null,
      "geo_enabled":true,
      "lang":"fr",
      "contributors_enabled":false,
      "is_translator":false,
      "profile_background_color":"C0DEED",
      "profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
      "profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
      "profile_background_tile":false,
      "profile_link_color":"0048B3",
      "profile_sidebar_border_color":"C0DEED",
      "profile_sidebar_fill_color":"DDEEF6",
      "profile_text_color":"333333",
      "profile_use_background_image":true,
      "profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/675445019294212096\/0d1ksXko_normal.jpg",
      "profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/675445019294212096\/0d1ksXko_normal.jpg",
      "profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/2586505687\/1434792576",
      "default_profile":false,
      "default_profile_image":false,
      "following":null,
      "follow_request_sent":null,
      "notifications":null
   },
   "geo":null,
   "coordinates":null,
   "place":{
      "id":"09ef78b32799b6e8",
      "url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/09ef78b32799b6e8.json",
      "place_type":"city",
      "name":"Orvault",
      "full_name":"Orvault, France",
      "country_code":"FR",
      "country":"France",
      "bounding_box":{
         "type":"Polygon",
         "coordinates":[
            [
               [
                  -1.663459,
                  47.239088
               ],
               [
                  -1.663459,
                  47.309610
               ],
               [
                  -1.576006,
                  47.309610
               ],
               [
                  -1.576006,
                  47.239088
               ]
            ]
         ]
      },
      "attributes":{

      }
   },
   "contributors":null,
   "is_quote_status":false,
   "retweet_count":0,
   "favorite_count":0,
   "entities":{
      "hashtags":[

      ],
      "urls":[

      ],
      "user_mentions":[
         {
            "screen_name":"julesbl99",
            "name":"jules moreaux",
            "id":787617474820841472,
            "id_str":"787617474820841472",
            "indices":[
               0,
               10
            ]
         }
      ],
      "symbols":[

      ]
   },
   "favorited":false,
   "retweeted":false,
   "filter_level":"low",
   "lang":"fr",
   "timestamp_ms":"1493213445337"
}

```


## Your Tasks

You and your team will have the task of reading in, cleaning, and exploring this dataset. Your job is to gain insight into what is happening during the time period.  Your task for today is to produce the following:

1. A python script containing helper functions.
You should be working toward transforming this large cumbersome dataset into something that is regular and easily digestible.  You need to find inconsistencies in the data, and try to think about how you would clean them.  You can do cleaning in data as they are RDDs, DataFrames, or ideally both, but the processes should be calling function that are reusable.

2. A presentation about your choices.
Later this afternoon you'll stop work and get together as a class to present your findings.  You can either choose to use slides or jupyter notebooks.  The latter might be nice, because you may want to highlight bits of code.


## Hints
### 1. Non-UTF Characters
We suggest reading in the data into spark RDDs, not directly into Dataframes.  You can do so using the ```textFile``` command from the ```SparkContext```, and then getting python dictionaries using the ```json``` class.  If you do a ```take(1)```, it should work just fine.  If, however, you try to do a count, you'll end up throwing an error.  This happens because the ```json``` class fails when you encounter the non-utf8 characters in the dataset.  To get around this, you should wrap the json decoding in a ```try - except``` block, and return ```None``` if an exception is hit.  You can then filter out none.

### 2. Messy Data
This will be the most challenging dataset you've had to work with up to this point.  The data is somewhat large, and tweets are a complicated and messy source of information. Your first steps should be to understand which fields you'll be leveraging.  Once you've read in the data, start by doing a ```take(1)``` to get a feel for what a tweet JSON string looks like.

Also note that data is messy so you'll need to do a lot of checks and filter out inconsistent data.  <b>Being able to adjust based on error messages is an important skill, consider this a chance to practice!</b>
