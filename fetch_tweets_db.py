import os
import sqlalchemy
import tweepy
import time

db_name = 'climate_change_tweets'
search_term = 'climate change'
output_tweets = '/home/sandra/Documents/CodingNomads/capstone/outputs/climate-change_tweets.json'
tweet_limit = 5000
batch_size = 500

# Check if database exists
password = os.environ['mySQLpwd']
engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost')
# Query for existing databases
existing_databases = engine.execute("SHOW DATABASES;")
existing_databases = [d[0] for d in existing_databases]
# Create database if does not exist
if db_name not in existing_databases:
    # create and activate database
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost')
    engine.execute(sqlalchemy.schema.CreateSchema(db_name))
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
    connection = engine.connect()
    metadata = sqlalchemy.MetaData()
    #create tables
    tweets = sqlalchemy.Table('tweets', metadata,
                           sqlalchemy.Column('id', sqlalchemy.BigInteger(), primary_key=True),
                           sqlalchemy.Column('created_at', sqlalchemy.String(50), nullable=False),
                           sqlalchemy.Column('text', sqlalchemy.String(288), nullable=False),
                           sqlalchemy.Column('user_id', sqlalchemy.BigInteger(), default=False)
                  )
    users = sqlalchemy.Table('users', metadata,
                           sqlalchemy.Column('user_id', sqlalchemy.BigInteger(), primary_key=True),
                           sqlalchemy.Column('screen_name', sqlalchemy.String(16), nullable=False),
                           sqlalchemy.Column('name', sqlalchemy.String(50), nullable=False),
                           sqlalchemy.Column('followers_count', sqlalchemy.Integer(), nullable=False),
                           sqlalchemy.Column('friends_count', sqlalchemy.Integer(), nullable=False)
                  )
    metadata.create_all(engine)
else:
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
    connection = engine.connect()
    metadata = sqlalchemy.MetaData()
    tweets = sqlalchemy.Table('tweets', metadata, autoload=True, autoload_with=engine)
    users = sqlalchemy.Table('users', metadata, autoload=True, autoload_with=engine)

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
        query = sqlalchemy.select([tweets.columns.id])
        result_proxy = connection.execute(query)
        tweet_result_set = result_proxy.fetchall()
        tweet_list = []
        for result in tweet_result_set:
            tweet_list.append(result['id'])
        if tweet._json['id'] not in tweet_list:
            # add tweet to database
            query_tweets = sqlalchemy.insert(tweets)
            new_tweet = [{'id': tweet._json['id'], 'created_at': tweet._json['created_at'], 'text': tweet._json['text'], 'user_id': tweet._json['user']['id']}]
            result_proxy = connection.execute(query_tweets, new_tweet)
            tweet_count += 1
        # check if user is in database
        query = sqlalchemy.select([users.columns.user_id])
        result_proxy = connection.execute(query)
        user_result_set = result_proxy.fetchall()
        user_list = []
        for result in user_result_set:
            user_list.append(result['user_id'])
        if tweet._json['user']['id'] not in user_list:
            # add user to database
            query_users = sqlalchemy.insert(users)
            new_user = [{'user_id': tweet._json['user']['id'], 'screen_name': tweet._json['user']['screen_name'], 'name': tweet._json['user']['name'], 'followers_count': tweet._json['user']['followers_count'], 'friends_count': tweet._json['user']['friends_count']}]
            result_proxy = connection.execute(query_users, new_user)
    if tweet_count < tweet_limit:
        time.sleep(60 * 15)
