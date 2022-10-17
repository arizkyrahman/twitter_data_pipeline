import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key = "le7W9O43lx0hMQKGTVNyX02WI"
    access_secret = "850Ut4R7WWlyvxgL0q0xGTnIquQjBgEHuro0SDt8RqhBiuFlLT"
    consumer_key = "1575058260604092417-R1hqYfYZnztPAUulhZhtOfGucGrmeq"
    consumer_secret = "Lm7t47zFE4QsUKOP7hZ10TnvH6CTFc4S0fIELwGt3jSAi"

    # twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                               count=200, include_rts=False, tweet_mode='extended')

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {'user': tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://rizky-pract1-bucket/elonmusk_twitter_data.csv")
