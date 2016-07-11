import database

application_configuration = database.get_application_configuration()
consumer_key = application_configuration['TwitterConsumerKey']
consumer_secret = application_configuration['TwitterConsumerSecret']
access_token_key = application_configuration['TwitterAccessTokenKey']
access_token_secret = application_configuration['TwitterAccessTokenSecret']
user_table_name = 'TwitterUser'
tweet_table_name = 'TwitterUserTweet'
tweet_hashtags_table_name = 'TwitterUserTweetHashtag'
tweet_urls_table_name = 'TwitterUserTweetUrl'
tweet_media_table_name = 'TwitterUserTweetMedia'
tweet_user_mentions_table_name = 'TwitterUserTweetUserMention'
media_table_name = 'TwitterMedia'
account_handle_table_name = 'AccountHandle'

timeline_count = 200

tweet_embedded_dict_keys = {
    'hashtags': 'text',
    'user_mentions': 'id',
    'media': 'id',
    'urls': 'expanded_url'
}

object_table_map = {
    'user': user_table_name,
    'media_obj': media_table_name,
    'tweet': tweet_table_name,
    'hashtags': tweet_hashtags_table_name,
    'urls': tweet_urls_table_name,
    'media': tweet_media_table_name,
    'user_mentions': tweet_user_mentions_table_name
}

object_id_map = {
    'user': 'user_id',
    'tweet': 'id'
}