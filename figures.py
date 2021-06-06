import os
import sqlalchemy
import markovify
import matplotlib.pyplot as plt
import numpy

db_name = 'climate_change_tweets'
password = os.environ['mySQLpwd']
# connect to tweet database
engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost/{db_name}')
connection = engine.connect()
metadata = sqlalchemy.MetaData()
metadata.reflect(bind=engine)
# load tables
tweets = sqlalchemy.Table('tweets', metadata, autoload=True, autoload_with=engine)
users = sqlalchemy.Table('users', metadata, autoload=True, autoload_with=engine)
join_statement = tweets.join(users, users.columns.user_id == tweets.columns.user_id)
# import tweet text
query = sqlalchemy.select(tweets.columns.text)
result_proxy = connection.execute(query)
data = result_proxy.fetchall()
# save tweet text to txt file
tweet_text = ''
for i in range(len(data)):
    tweet_text += data[i][0] + '\n'
with open('tweets_text.txt', 'w') as f:
    f.writelines(tweet_text)
# make word cloud using command line
# wordcloud_cli --text tweets_text.txt --imagefile tweet_cloud.png

# generate new tweets with markovify
# Build the model.
text_model = markovify.Text(tweet_text)
# Print three randomly-generated tweets of no more than 280 characters
for i in range(3):
    print(text_model.make_short_sentence(280))

# make bar plot of users' tweet numbers
query = sqlalchemy.select([users.columns.screen_name, sqlalchemy.func.count(tweets.columns.id)]).select_from(join_statement).group_by(users.columns.user_id).order_by(sqlalchemy.func.count(tweets.columns.id).desc()).limit(100)
result_proxy = connection.execute(query)
tweets_per_user = result_proxy.fetchall()
tweet_counts = [x[1] for x in tweets_per_user]

y_pos = numpy.arange(len(tweet_counts))

plt.bar(y_pos, tweet_counts, align='center', alpha=0.5)
plt.ylabel('Number of Tweets')
plt.title('Distribution of Tweets by Top 100 Tweeters')

plt.savefig('tweets_per_user.png')
