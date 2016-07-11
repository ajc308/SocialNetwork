import services.twitter.config as twitter_config
import services.twitter.utils as twitter_utils

from services.twitter.api import TwitterAPI


twitter_api = TwitterAPI(consumer_key=twitter_config.consumer_key,
                         consumer_secret=twitter_config.consumer_secret,
                         access_token_key=twitter_config.access_token_key,
                         access_token_secret=twitter_config.access_token_secret)


twitter_utils.get_new_users_from_handles(twitter_api)
twitter_utils.get_latest_tweets_for_users(twitter_api)

twitter_utils.sync_object_by_id(
    api=twitter_api,
    object_type='user',
    where_string='GetUserInfo = 1 AND DateDiff(day, GETDATE(), UpdatedOn) IS NULL OR DateDiff(day, GETDATE(), UpdatedOn) > 2'
)  # Run once or twice a day, due to rate limiting



# TODO
# Tweet coordinates
# Media/User Mentions not inserting reference if just 1? - Ex. tweet id 678952649551688000