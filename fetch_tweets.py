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
    time.sleep(60 * 15)

# write tweet data to a JSON file
with open(output_tweets, 'w') as f:
    json.dump(tweet_list, f)
