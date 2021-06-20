"""
fetch_tweets_db.py is a command line script designed to search Twitter
using a Twitter development account and the tweepy package.
Twitter access keys are saved as environmental variables capstoneAPI,
capstoneAPISecret, capstoneAccess, and capstoneAccessSecret.

Tweets are downloaded in batches (batch_size) every 15 minutes to
comply with Twitter rate limits. The total number of tweets downloaded
is determined by the tweet_limit. The search_term is the word or
phrase used to search Twitter.

Downloaded tweets are saved in a MySQL database (db_name) using the
package SQLalchemy. MySQL user is set to root and MySQL password is
saved in the environmental variable mySQLpwd.

This script uses the default tweet length, which truncates all tweets
after the first 140 characters.

Required packages: tweepy, SQLalchemy, MySQL
"""

import os
from sqlalchemy import create_engine, schema, MetaData, Table, Column, \
    BigInteger, Integer, String, select, insert
import tweepy
import time

db_name = 'climate_change_tweets'
search_term = 'climate change'
tweet_limit = 5000
batch_size = 500

# Check if database exists
password = os.environ['mySQLpwd']
engine = create_engine(f'mysql+pymysql://root:{password}@localhost')

# Query for existing databases
existing_databases = engine.execute("SHOW DATABASES;")
existing_databases = [d[0] for d in existing_databases]

# Create database if does not exist
if db_name not in existing_databases:

    # create and activate database
    engine = create_engine(f'mysql+pymysql://root:{password}@localhost')
    engine.execute(schema.CreateSchema(db_name))
    engine = create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
    connection = engine.connect()
    metadata = MetaData()

    #create tables
    tweets = Table('tweets', metadata,
                           Column('id', BigInteger(), primary_key=True),
                           Column('created_at', String(50), nullable=False),
                           Column('text', String(144), nullable=False),
                           Column('user_id', BigInteger(), default=False)
                  )

    users = Table('users', metadata,
                           Column('user_id', BigInteger(), primary_key=True),
                           Column('screen_name', String(16), nullable=False),
                           Column('name', String(50), nullable=False),
                           Column('followers_count', Integer(), nullable=False),
                           Column('friends_count', Integer(), nullable=False)
                  )

    metadata.create_all(engine)

else:
    # connect to existing database
    engine = create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
    connection = engine.connect()
    metadata = MetaData()

    tweets = Table('tweets', metadata, autoload=True, autoload_with=engine)
    users = Table('users', metadata, autoload=True, autoload_with=engine)

# authenticate to the service we're accessing
auth = tweepy.OAuthHandler(os.environ['capstoneAPI'], os.environ['capstoneAPISecret'])
auth.set_access_token(os.environ['capstoneAccess'], os.environ['capstoneAccessSecret'])

# create the connection
api = tweepy.API(auth)

# get tweets in batches of 500 every 15 minutes
tweet_count = 0

while tweet_count < tweet_limit:

    for tweet in tweepy.Cursor(api.search, q=search_term, lang="en", since=2021-1-1).items(batch_size):

        # check if tweet is in database
        query = select([tweets.columns.id])
        result_proxy = connection.execute(query)
        tweet_result_set = result_proxy.fetchall()
        tweet_list = []

        for result in tweet_result_set:
            tweet_list.append(result['id'])

        if tweet._json['id'] not in tweet_list:

            # add tweet to database
            query_tweets = insert(tweets)
            new_tweet = [{'id': tweet._json['id'], 'created_at': tweet._json['created_at'],
                          'text': tweet._json['text'], 'user_id': tweet._json['user']['id']}]

            result_proxy = connection.execute(query_tweets, new_tweet)
            tweet_count += 1

        # check if user is in database
        query = select([users.columns.user_id])
        result_proxy = connection.execute(query)
        user_result_set = result_proxy.fetchall()
        user_list = []

        for result in user_result_set:
            user_list.append(result['user_id'])

        if tweet._json['user']['id'] not in user_list:

            # add user to database
            query_users = insert(users)

            new_user = [{'user_id': tweet._json['user']['id'],
                         'screen_name': tweet._json['user']['screen_name'],
                         'name': tweet._json['user']['name'],
                         'followers_count': tweet._json['user']['followers_count'],
                         'friends_count': tweet._json['user']['friends_count']}]

            result_proxy = connection.execute(query_users, new_user)

    if tweet_count < tweet_limit:
        time.sleep(60 * 15)
