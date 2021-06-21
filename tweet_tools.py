"""
This program allows the user to collect tweets from Twitter, store them in a
file or database, and analyze the collected tweets. Tweets are collected in
extended mode, which allows for tweets longer than 140 characters. However,
the database function truncates tweets to 288 characters.

Metrics calculated:
average_followers: the average number of followers for the tweeters of each tweet
average_words: the average number of words in each tweet
average_characters: the average number of characters in each tweet
percent_hashtags: the percent of tweets that contain hashtags (#)
percent_mentions: the percent of tweets that mention another user (@)
percent_punctuated: the percent of tweets with punctuation
most_tweets: the user with the most tweets in the dataset
average_tweets: the average number of tweets per user
busiest_hour: the hour of the day with the most tweets
longest_word: the longest word found in any tweet
shortest_word: the shortest word found in any tweet
most common words: the 100 most common words from the tweets
most common symbols: the 100 most common symbols from the tweets

Other analyses for databases:
Create word cloud from tweet text
Generate artificial tweets from tweet text
Plot tweets per user for top 100 tweeters

Keys and passwords are stored as environmental variables.
Twitter access keys are saved as environmental variables capstoneAPI,
capstoneAPISecret, capstoneAccess, and capstoneAccessSecret.
MySQL password is saved as mySQLpwd.

Requires: Twitter developer account, MySQL, sqlalchemy, tweepy, wordcloud, markovify, matplotlib, numpy

To run: python3 tweet_tools.py
"""

import os
from sqlalchemy import create_engine, schema, MetaData, Table, Column, \
    BigInteger, Integer, String, select, insert, func, exc
import tweepy
import time
import json
import string
import datetime
import markovify
import matplotlib.pyplot as plot
import numpy

def authenticate():
    """Creates a connection to Twitter using keys stored as environmental variables"""
    # authenticate to the service
    auth = tweepy.OAuthHandler(os.environ['capstoneAPI'], os.environ['capstoneAPISecret'])
    auth.set_access_token(os.environ['capstoneAccess'], os.environ['capstoneAccessSecret'])

    # return the connection
    return tweepy.API(auth)

def connect_db(db_name):
    """Connects to database and creates database if does not exist"""
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

        # create tables
        tweets = Table('tweets', metadata,
                       Column('id', BigInteger(), primary_key=True),
                       Column('created_at', String(50), nullable=False),
                       Column('text', String(288), nullable=False),
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
    return connection, tweets, users


def fetch_tweets_json(search_term, output_tweets, tweet_limit, batch_size = 500):
    """Searches Twitter for tweet_limit tweets containing search_term and saves
    them to a json file called output_tweets"""
    # create the connection
    api = authenticate()

    # get tweets in batches of 500 every 15 minutes
    tweet_list = []
    while len(tweet_list) < tweet_limit:
        print(f'Collecting batch of {batch_size} tweets...')
        for tweet in tweepy.Cursor(api.search, q=search_term, tweet_mode='extended',
                                   lang="en", since=2021 - 1 - 1).items(batch_size):
            tweet_list.append(tweet._json)

        print(f'{len(tweet_list)} tweets collected')

        if len(tweet_list) < tweet_limit:
            print('Pausing for 15 minutes...')
            time.sleep(60 * 15)


    # write tweet data to a JSON file
    with open(output_tweets, 'w') as f:
        json.dump(tweet_list, f)


def fetch_tweets_db(search_term, db_name, tweet_limit, batch_size=500):
    """Search Twitter for tweet_limit tweets containing search_term and store them
    in a MySQL database named db_name"""

    # create the connection
    api = authenticate()

    # connect to database
    connection, tweets, users = connect_db(db_name)

    # get tweets in batches of 500 every 15 minutes
    tweet_count = 0

    while tweet_count < tweet_limit:
        print(f'Collecting batch of {batch_size} tweets...')

        for tweet in tweepy.Cursor(api.search, q=search_term, tweet_mode='extended', lang="en", since=2021 - 1 - 1).items(batch_size):

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
                              'text': tweet._json['full_text'][:287], 'user_id': tweet._json['user']['id']}]

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

        print(f'{tweet_count} tweets collected')

        if tweet_count < tweet_limit:
            print('Pausing for 15 minutes...')
            time.sleep(60 * 15)


def analyze_tweets_json(filename, output):
    """Calculate tweet metrics from json file"""
    # import tweet file
    with open(filename, 'r') as f:
        data = json.load(f)

    # gather metrics by iterating over tweets
    sum_followers = 0
    sum_words = 0
    sum_characters = 0
    count_hashtags = 0
    count_mentions = 0
    word_dict = {}
    symbol_dict = {}
    nonsymbols = string.ascii_letters + string.digits
    count_punctuated = 0
    user_dict = {}
    hour_dict = {}

    for tweet in range(len(data)):
        # add number of followers to running total to calculate average followers
        sum_followers += data[tweet]['user']['followers_count']

        # add number of characters to running total to calculate average characters
        sum_characters += len(data[tweet]['full_text'])

        # add to count of tweets with hashtags if # found
        if data[tweet]['full_text'].find('#') > -1:
            count_hashtags += 1

        # add to count of tweets with mentions if @ found
        if data[tweet]['full_text'].find('@') > -1:
            count_mentions += 1

        # add to count of tweets with punctuation if punctuation found
        if any(p in data[tweet]['full_text'] for p in string.punctuation):
            count_punctuated += 1

        # count occurrence of each symbol in tweets
        for char in data[tweet]['full_text']:
            if char not in nonsymbols:
                if char in symbol_dict.keys():
                    symbol_dict[char] += 1
                else:
                    symbol_dict[char] = 1

        # remove punctuation and split text into words
        words = data[tweet]['full_text'].translate(str.maketrans('', '', string.punctuation)).upper().split()

        # add number of words to running total to calculate average words
        sum_words += len(words)

        # count occurrences of each word in tweets, not including URLs
        for word in words:
            if word.find('HTTP') == -1 and word.isalpha():
                if word in word_dict.keys():
                    word_dict[word] += 1
                else:
                    word_dict[word] = 1

        # add tweet to user's tweet count
        if data[tweet]['user']['screen_name'] in user_dict.keys():
            user_dict[data[tweet]['user']['screen_name']] += 1

        else:
            user_dict[data[tweet]['user']['screen_name']] = 1

        # add tweet to hourly count "created_at": "Thu Dec 15 18:31:34 +0000 2016"
        if data[tweet]['created_at'][11:13] in hour_dict.keys():
            hour_dict[data[tweet]['created_at'][11:13]] += 1

        else:
            hour_dict[data[tweet]['created_at'][11:13]] = 1

    # The average number of followers.
    average_followers = sum_followers / len(data)

    # The average length of tweets (counting words).
    average_words = sum_words / len(data)

    # The average length of tweets (counting characters).
    average_characters = sum_characters / len(data)

    # The percentage of tweets that have a hashtag (#).
    percent_hashtags = (count_hashtags / len(data)) * 100

    # The percentage of tweets that have a mention (@).
    percent_mentions = (count_mentions / len(data)) * 100

    # The 100 most common words.
    words_sorted = sorted(word_dict, key=lambda item: word_dict[item], reverse=True)

    # The 100 most common symbols.
    symbols_sorted = sorted(symbol_dict, key=lambda item: symbol_dict[item], reverse=True)

    # Percentage of tweets that use punctuation.
    percent_punctuated = (count_punctuated / len(data)) * 100

    # The longest and shortest word in a tweet.
    longest_count = 0
    longest_word = []
    shortest_count = 100
    shortest_word = []

    for m in word_dict.keys():
        # find longest word
        if len(m) > longest_count:
            longest_count = len(m)
            longest_word = [m]

        elif len(m) == longest_count:
            longest_word.append(m)

        # find shortest word
        if len(m) < shortest_count:
            shortest_count = len(m)
            shortest_word = [m]

        elif len(m) == shortest_count:
            shortest_word.append(m)

    # What user has the most tweets in the dataset?
    users_sorted = sorted(user_dict, key=lambda item: user_dict[item], reverse=True)

    # The average number of tweets from an individual user.
    average_tweets = len(data) / len(user_dict.keys())

    # The hour with the greatest number of tweets.
    hours_sorted = sorted(hour_dict, key=lambda item: hour_dict[item], reverse=True)

    with open(output, 'w') as f:
        f.write(f"There are {len(data)} tweets in the dataset.\n"
                f"The average number of followers that users have is {average_followers} followers.\n"
                f"The average length of the tweets is {average_words} words and {average_characters} characters.\n"
                f"{percent_hashtags}% of tweets contain a hashtag.\n"
                f"{percent_mentions}% of tweets contain a mention.\n"
                f"The 100 most common words:\n{' '.join(words_sorted[:99])}\n"
                f"The 100 most common symbols: {' '.join(symbols_sorted[:99])}\n"
                f"{percent_punctuated}% of tweets use punctuation.\n"
                f"The longest word(s) is/are {', '.join(z for z in longest_word)} at {longest_count} letters.\n"
                f"The shortest word(s) is/are {', '.join(s for s in shortest_word)}.\n"
                f"{users_sorted[0]} has the most tweets.\n"
                f"Users average {average_tweets} tweets.\n"
                f"The busiest time for tweeting is {hours_sorted[0]}:00."
                )

def analyze_tweets_db(db_name):
    """Calculate tweet metrics from MySQL database"""
    # connect to tweet database
    engine = create_engine(f'mysql+pymysql://root:{os.environ["mySQLpwd"]}@localhost/{db_name}')
    connection = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)

    tweets = Table('tweets', metadata, autoload=True, autoload_with=engine)
    users = Table('users', metadata, autoload=True, autoload_with=engine)

    # gather metrics by iterating over tweets
    sum_words = 0
    sum_characters = 0
    count_hashtags = 0
    count_mentions = 0
    word_dict = {}
    symbol_dict = {}
    nonsymbols = string.ascii_letters + string.digits
    count_punctuated = 0

    join_statement = tweets.join(users, users.columns.user_id == tweets.columns.user_id)
    query = select(tweets.columns.text)
    result_proxy = connection.execute(query)
    data = result_proxy.fetchall()

    for tweet in range(len(data)):
        # add number of characters to running total to calculate average characters
        sum_characters += len(data[tweet][0])
        # add to count of tweets with hashtags if # found
        if data[tweet][0].find('#') > -1:
            count_hashtags += 1
        # add to count of tweets with mentions if @ found
        if data[tweet][0].find('@') > -1:
            count_mentions += 1
        # add to count of tweets with punctuation if punctuation found
        if any(p in data[tweet][0] for p in string.punctuation):
            count_punctuated += 1
        # count occurrence of each symbol in tweets
        for c in data[tweet][0]:
            if c not in nonsymbols:
                if c in symbol_dict.keys():
                    symbol_dict[c] += 1
                else:
                    symbol_dict[c] = 1
        # remove punctuation and split text into words
        words = data[tweet][0].translate(str.maketrans('', '', string.punctuation)).upper().split()
        # add number of words to running total to calculate average words
        sum_words += len(words)
        # count occurrences of each word in tweets, not including URLs
        for word in words:
            if word.find('HTTP') == -1 and word.isalpha():
                if word in word_dict.keys():
                    word_dict[word] += 1
                else:
                    word_dict[word] = 1

    # The average number of followers.
    query = select(func.avg(users.columns.followers_count)).select_from(join_statement)
    result_proxy = connection.execute(query)
    average_followers = float(result_proxy.fetchone()[0])

    # The average length of tweets (counting words).
    average_words = sum_words / len(data)

    # The average length of tweets (counting characters).
    average_characters = sum_characters / len(data)

    # The percentage of tweets that have a hashtag (#).
    percent_hashtags = (count_hashtags / len(data)) * 100

    # The percentage of tweets that have a mention (@).
    percent_mentions = (count_mentions / len(data)) * 100

    # The 100 most common words.
    words_sorted = sorted(word_dict, key=lambda item: word_dict[item], reverse=True)[0:99]

    # The 100 most common symbols.
    symbols_sorted = sorted(symbol_dict, key=lambda item: symbol_dict[item], reverse=True)[0:99]

    # Percentage of tweets that use punctuation.
    percent_punctuated = (count_punctuated / len(data)) * 100

    # The longest and shortest word in a tweet.
    longest_count = 0
    longest_word = []
    shortest_count = 100
    shortest_word = []

    for m in word_dict.keys():
        # find longest word
        if len(m) > longest_count:
            longest_count = len(m)
            longest_word = [m]

        elif len(m) == longest_count:
            longest_word.append(m)

        # find shortest word
        if len(m) < shortest_count:
            shortest_count = len(m)
            shortest_word = [m]

        elif len(m) == shortest_count:
            shortest_word.append(m)

    # Which user has the most tweets in the dataset?
    query = select([users.columns.user_id, func.count(tweets.columns.id)]). \
        select_from(join_statement).group_by(users.columns.user_id). \
        order_by(func.count(tweets.columns.id).desc())

    result_proxy = connection.execute(query)
    tweets_per_user = result_proxy.fetchall()
    most_tweets = tweets_per_user[0][0]

    # The average number of tweets from an individual user.
    average_tweets = len(data) / len(tweets_per_user)

    # The hour with the greatest number of tweets.
    query = select(tweets.columns.created_at)
    result_proxy = connection.execute(query)
    timestamps = result_proxy.fetchall()
    hour_dict = {}

    for t in timestamps:
        if t[0][11:13] in hour_dict.keys():
            hour_dict[t[0][11:13]] += 1
        else:
            hour_dict[t[0][11:13]] = 1

    busiest_hour = sorted(hour_dict, key=lambda item: hour_dict[item], reverse=True)[0]

    # add metrics to database
    # check if metrics table exists and create it if it doesn't exist
    if 'metrics' not in metadata.tables.keys():
        # create metrics table
        metrics = Table('metrics', metadata,
                        Column('analyzed_at', String(50), primary_key=True),
                        Column('number_analyzed', Integer, nullable=False),
                        Column('average_followers', Integer, nullable=False),
                        Column('average_words', Integer, default=False),
                        Column('average_characters', Integer, nullable=False),
                        Column('percent_hashtags', Integer, nullable=False),
                        Column('percent_mentions', Integer, default=False),
                        Column('percent_punctuated', Integer, nullable=False),
                        Column('most_tweets', BigInteger, nullable=False),
                        Column('average_tweets', Integer, default=False),
                        Column('busiest_hour', String(2), nullable=False),
                        Column('longest_word', String(280), nullable=False),
                        Column('shortest_word', String(78), default=False),
                        )

        common_words = Table('common_words', metadata,
                             Column('word_id', Integer, autoincrement=True, primary_key=True),
                             Column('analyzed_at', String(50), nullable=False),
                             Column('word', String(50), nullable=False),
                             )

        common_symbols = Table('common_symbols', metadata,
                               Column('symbol_id', Integer, autoincrement=True, primary_key=True),
                               Column('analyzed_at', String(50), nullable=False),
                               Column('symbol', String(5), nullable=False),
                               )

        metadata.create_all(engine)

    else:
        # load existing metrics tables from database
        metrics = Table('metrics', metadata, autoload=True, autoload_with=engine)
        common_words = Table('common_words', metadata, autoload=True, autoload_with=engine)
        common_symbols = Table('common_symbols', metadata, autoload=True, autoload_with=engine)

    # add metrics to metrics table with timestamp as key
    analysis_time = str(datetime.datetime.now())

    query = insert(metrics).values(analyzed_at=analysis_time, number_analyzed=len(data),
                                   average_followers=average_followers, average_words=average_words,
                                   average_characters=average_characters, percent_hashtags=percent_hashtags,
                                   percent_mentions=percent_mentions, percent_punctuated=percent_punctuated,
                                   most_tweets=most_tweets, average_tweets=average_tweets,
                                   busiest_hour=busiest_hour, longest_word=longest_word[0],
                                   shortest_word=shortest_word[0])

    result_proxy = connection.execute(query)

    # add 100 most common words to common_words table
    new_words = []

    for w in words_sorted:
        new_words.append({'analyzed_at': analysis_time, 'word': w})

    query = insert(common_words)
    result_proxy = connection.execute(query, new_words)

    # add 100 most common symbols to common_symbols table
    new_symbols = []

    for s in symbols_sorted:
        new_symbols.append({'analyzed_at': analysis_time, 'symbol': s})

    query = insert(common_symbols)
    result_proxy = connection.execute(query, new_symbols)

def get_tweet_text(db_name):
    """Collects text from all tweets in a database"""
    # connect to tweet database
    engine = create_engine(f'mysql+pymysql://root:{os.environ["mySQLpwd"]}@localhost/{db_name}')
    connection = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # load tables
    tweets = Table('tweets', metadata, autoload=True, autoload_with=engine)
    users = Table('users', metadata, autoload=True, autoload_with=engine)

    # import tweet text
    query = select(tweets.columns.text)
    result_proxy = connection.execute(query)
    data = result_proxy.fetchall()

    # save tweet text to txt file
    tweet_text = ''
    for tweet in range(len(data)):
        tweet_text += data[tweet][0] + '\n'

    return tweet_text

def get_tweets_per_user(db_name):
    """Collects tweet numbers from all users in a database"""
    # connect to tweet database
    engine = create_engine(f'mysql+pymysql://root:{os.environ["mySQLpwd"]}@localhost/{db_name}')
    connection = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # load tables
    tweets = Table('tweets', metadata, autoload=True, autoload_with=engine)
    users = Table('users', metadata, autoload=True, autoload_with=engine)
    join_statement = tweets.join(users, users.columns.user_id == tweets.columns.user_id)

    # query database
    query = select([users.columns.screen_name, func.count(tweets.columns.id)]). \
        select_from(join_statement).group_by(users.columns.user_id). \
        order_by(func.count(tweets.columns.id).desc()).limit(100)
    result_proxy = connection.execute(query)
    tweets_per_user = result_proxy.fetchall()
    return [x[1] for x in tweets_per_user]

def list_schema():
    """List available MySQL schema"""
    # Check if database exists
    password = os.environ['mySQLpwd']
    engine = create_engine(f'mysql+pymysql://root:{password}@localhost')

    # Query for existing databases
    existing_databases = engine.execute("SHOW DATABASES;")
    return [d[0] for d in existing_databases]

# user interface
status = 0
while status != 99:
    try:
        choice = int(input('Choose an option:\n'
                           '1) Fetch tweets and save to file\n'
                           '2) Fetch tweets and save to MySQL database\n'
                           '3) Analyze tweets from file\n'
                           '4) Analyze tweets from database\n'
                           '5) Create word cloud from database\n'
                           '6) Generate tweets from database\n'
                           '7) Plot tweets per user from database\n'
                           '8) Display available databases\n'
                           '99) Exit\n'))
    except ValueError:
        choice = 0

    if choice == 1:
        # Fetch tweets and save to json file
        search_term = input('Enter your search term: ')
        output_tweets = input('Enter the output filename: ')
        tweet_limit = int(input('Enter the number of tweets to collect: '))
        fetch_tweets_json(search_term, output_tweets, tweet_limit)

    elif choice == 2:
        # Fetch tweets and add to database
        search_term = input('Enter your search term: ')
        db_name = input('Enter the database name: ')
        tweet_limit = int(input('Enter the number of tweets to collect: '))
        fetch_tweets_db(search_term, db_name, tweet_limit)

    elif choice == 3:
        # Analyze tweets from json file
        filename = input("Enter the file containing the tweets: ")
        output = input("Enter the output file name: ")
        try:
            analyze_tweets_json(filename, output)
        except FileNotFoundError:
            print('File not found')

    elif choice == 4:
        # Analyze tweets from database
        db_name = input('Enter the tweet database you would like to analyze: ')
        try:
            analyze_tweets_db(db_name)
        except exc.OperationalError:
            print('Database not found')

    elif choice == 5:
        # Create word cloud
        db_name = input('Enter database you would like to analyze: ')
        cloud_name = input('Enter filename for word cloud (.png): ')

        # check file extension
        if cloud_name[-4:] != '.png':
            cloud_name = cloud_name + '.png'

        try:
            # Get text
            tweet_text = get_tweet_text(db_name)

            # Write text to file
            with open('tweets_text.txt', 'w') as f:
                f.writelines(tweet_text)

            # create word cloud
            os.system(f'wordcloud_cli --text tweets_text.txt --imagefile {cloud_name}')
        except exc.OperationalError:
            print('Database not found')

    elif choice == 6:
        # Generate tweets
        db_name = input('Enter database you would like to analyze: ')
        num_tweets = int(input('Enter number of tweets to generate: '))

        try:
            # Get text
            tweet_text = get_tweet_text(db_name)

            # Create model
            text_model = markovify.Text(tweet_text)

            # Print three randomly-generated tweets of no more than 280 characters
            for i in range(num_tweets):
                print(text_model.make_short_sentence(280))

        except exc.OperationalError:
            print('Database not found')

    elif choice == 7:
        # plot tweets per user
        db_name = input('Enter database you would like to analyze: ')
        plot_name = input('Enter name of output file (.png): ')

        # check file extension
        if plot_name[-4:] != '.png':
            plot_name = plot_name + '.png'

        try:
            # make bar plot of users' tweet numbers
            tweet_counts = get_tweets_per_user(db_name)
            y_pos = numpy.arange(len(tweet_counts))

            plot.bar(y_pos, tweet_counts, align='center', alpha=0.5)
            plot.ylabel('Number of Tweets')
            plot.title('Distribution of Tweets by Top 100 Tweeters')

            # save plot to file
            plot.savefig(plot_name)

        except exc.OperationalError:
            print('Database not found')

    elif choice == 8:
        # list MySQL databases
        print('\n'.join(list_schema()))

    elif choice == 99:
        status = 99

    else:
        print('That is not an option')
