"""
fetch_tweets.py is a command line script designed to search Twitter
using a Twitter development account and the tweepy package.
Twitter access keys are saved as environmental variables capstoneAPI,
capstoneAPISecret, capstoneAccess, and capstoneAccessSecret.

Tweets are downloaded in batches (batch_size) every 15 minutes to
comply with Twitter rate limits. The total number of tweets downloaded
is determined by the tweet_limit. The search_term is the word or
phrase used to search Twitter.

Downloaded tweets are saved in a json file (output_tweets) for later
analysis.

This script uses the default tweet length, which truncates all tweets
after the first 140 characters.

Required packages: tweepy
"""

import os
import tweepy
import json
import time

search_term = 'climate change'
output_tweets = 'climate-change_tweets.json'
tweet_limit = 5000
batch_size = 500

# authenticate to the service we're accessing
auth = tweepy.OAuthHandler(os.environ['capstoneAPI'], os.environ['capstoneAPISecret'])
auth.set_access_token(os.environ['capstoneAccess'], os.environ['capstoneAccessSecret'])

# create the connection
api = tweepy.API(auth)

# get tweets in batches of 500 every 15 minutes
tweet_list = []
while len(tweet_list) < tweet_limit:
    for tweet in tweepy.Cursor(api.search, q=search_term, lang="en", since=2021-1-1).items(batch_size):
        tweet_list.append(tweet._json)
    if len(tweet_list) < tweet_limit:
        time.sleep(60 * 15)

# write tweet data to a JSON file
with open(output_tweets, 'w') as f:
    json.dump(tweet_list, f)
