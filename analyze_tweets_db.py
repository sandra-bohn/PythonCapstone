"""
This command line script retrieves tweets from a database (db_name) and
calculates a series of metrics that are then saved to the metrics table
in the same database.

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

Tweets are retrieved from a MySQL database (db_name) using the
package SQLalchemy. MySQL user is set to root and MySQL password is
saved in the environmental variable mySQLpwd.

Required package: sqlalchemy
"""
import os
from sqlalchemy import create_engine, MetaData, Table, select, func, \
    Column, String, Integer, BigInteger, insert
import string
import datetime

db_name = 'climate_change_tweets'
password = os.environ['mySQLpwd']

# connect to tweet database
engine = create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
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
query = select([users.columns.user_id, func.count(tweets.columns.id)]).\
    select_from(join_statement).group_by(users.columns.user_id).\
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
