"""
This command line script retrieves tweets from a json file (filename) and
calculates a series of metrics that are then saved to a report txt file (output).

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
"""
import json
import string

filename = 'climate-change_tweets.json'
output = 'climate-change_tweets_summary.txt'

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
    sum_characters += len(data[tweet]['text'])

    # add to count of tweets with hashtags if # found
    if data[tweet]['text'].find('#') > -1:
        count_hashtags += 1

    # add to count of tweets with mentions if @ found
    if data[tweet]['text'].find('@') > -1:
        count_mentions += 1

    # add to count of tweets with punctuation if punctuation found
    if any(p in data[tweet]['text'] for p in string.punctuation):
        count_punctuated += 1

    # count occurrence of each symbol in tweets
    for char in data[tweet]['text']:
        if char not in nonsymbols:
            if char in symbol_dict.keys():
                symbol_dict[char] += 1
            else:
                symbol_dict[char] = 1

    # remove punctuation and split text into words
    words = data[tweet]['text'].translate(str.maketrans('', '', string.punctuation)).upper().split()

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
