"""
This command line script creates three outputs from tweets
stored in a database: a word cloud from tweet text, Markov
generated artificial tweets, and a plot showing the number
of tweets per user for the top 100 tweeters.

Tweets are retrieved from a MySQL database (db_name) using the
package SQLalchemy. MySQL user is set to root and MySQL password is
saved in the environmental variable mySQLpwd.

Once the tweet text has been saved to a txt file, the word
cloud is generated from the command line.

Required packages: sqlalchemy, markovify, matplotlib, numpy
"""

import os
from sqlalchemy import create_engine, MetaData, Table, select, func
import markovify
import matplotlib.pyplot as plot
import numpy

db_name = 'climate_change_tweets'
password = os.environ['mySQLpwd']

# connect to tweet database
engine = create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
connection = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)

# load tables
tweets = Table('tweets', metadata, autoload=True, autoload_with=engine)
users = Table('users', metadata, autoload=True, autoload_with=engine)
join_statement = tweets.join(users, users.columns.user_id == tweets.columns.user_id)

# import tweet text
query = select(tweets.columns.text)
result_proxy = connection.execute(query)
data = result_proxy.fetchall()

# save tweet text to txt file
tweet_text = ''
for i in range(len(data)):
    tweet_text += data[i][0] + '\n'
with open('tweets_text.txt', 'w') as f:
    f.writelines(tweet_text)

## WORD CLOUD
# make word cloud using command line
# wordcloud_cli --text tweets_text.txt --imagefile tweet_cloud.png


## GENERATE TWEETS
# generate new tweets with markovify
# Build the model.
text_model = markovify.Text(tweet_text)
# Print three randomly-generated tweets of no more than 280 characters
for i in range(3):
    print(text_model.make_short_sentence(280))


## BAR PLOT
# make bar plot of users' tweet numbers
query = select([users.columns.screen_name, func.count(tweets.columns.id)]).\
    select_from(join_statement).group_by(users.columns.user_id).\
    order_by(func.count(tweets.columns.id).desc()).limit(100)
result_proxy = connection.execute(query)
tweets_per_user = result_proxy.fetchall()
tweet_counts = [x[1] for x in tweets_per_user]

y_pos = numpy.arange(len(tweet_counts))

plot.bar(y_pos, tweet_counts, align='center', alpha=0.5)
plot.ylabel('Number of Tweets')
plot.title('Distribution of Tweets by Top 100 Tweeters')

# save plot to file
plot.savefig('tweets_per_user.png')
