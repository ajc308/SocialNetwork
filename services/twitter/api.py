import twitter


class TwitterAPI:
    def __init__(self, consumer_key, consumer_secret, access_token_key, access_token_secret):
        self.api = twitter.Api(consumer_key=consumer_key,
                               consumer_secret=consumer_secret,
                               access_token_key=access_token_key,
                               access_token_secret=access_token_secret
        )
        self.api.InitializeRateLimit()

    def check_rate_limits(self):
        for resource in sorted(self.api.rate_limit.resources):
            print(resource)
            for endpoint in sorted(self.api.rate_limit.resources[resource]):
                print('\t', endpoint, self.api.rate_limit.resources[resource][endpoint])
            print('\n')

    def get_user(self, kwargs):
        return self.api.GetUser(**kwargs)

    def get_user_timeline(self, kwargs):
        return self.api.GetUserTimeline(**kwargs)

    def map_obj_get_method(self, object_type):
        object_map = {
            'user': self.get_user
        }
        return object_map[object_type]