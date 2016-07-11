import database
import pymssql
import services.twitter.config as twitter_config
import twitter


def object_data_dict_item_id(object_data):
    for key in object_data:
        if type(object_data[key]) == dict:
            try:
                object_data[key + '_id'] = object_data[key]['id']
                object_data.pop(key)
            except:
                print(key + ' does not have id field. Likely deprecated.')
        elif type(object_data[key]) == list:
            object_data[key] = ', '.join(map(str, [obj[twitter_config.tweet_embedded_dict_keys[key]] for obj in object_data[key]]))

    return object_data


def filter_valid_columns(object_data, table_name):
    object_columns = object_data.keys()
    table_columns = database.get_table_columns(table_name)
    valid_columns = list(set(object_columns).intersection(set(table_columns)))
    return valid_columns


def insert_database_object_from_api(obj, object_type):
    object_data = obj.AsDict() if type(obj) != dict else obj
    table_name = twitter_config.object_table_map[object_type.lower()]
    object_data = object_data_dict_item_id(object_data)
    valid_columns = filter_valid_columns(object_data, table_name)

    insert_query = database.generate_insert_query(
        table_name=table_name,
        columns=valid_columns,
        data=object_data
    )

    try:
        database.execute_query(insert_query)
    except pymssql.IntegrityError:
        pass
    except Exception as e:
        print(insert_query)
        print(object_data)
        print(valid_columns)
        print(e)


def update_database_object_from_api(obj, object_type, where_data):
    object_data = obj.AsDict()
    table_name = twitter_config.object_table_map[object_type.lower()]
    object_data = object_data_dict_item_id(object_data)
    valid_columns = filter_valid_columns(object_data, table_name)

    update_query = database.generate_update_query(
        table_name=table_name,
        columns=valid_columns,
        data=object_data,
        where_data=where_data
    )

    database.execute_query(update_query)


def sync_object_by_id( api, object_type, where_data=None, where_string=None):
    table_name = twitter_config.object_table_map[object_type.lower()]

    object_ids = database.select_from_table(
        table_name=table_name,
        column_names=['id'],
        where_data=where_data,
        where_string=where_string,
        as_dict=True
    )
    for count_o, obj in enumerate(object_ids, 0):
        print('{} #{} of {} - {}'.format(object_type, count_o+1, len(object_ids), obj))
        obj_id_name = twitter_config.object_id_map[object_type.lower()]
        api_object = api.map_obj_get_method(object_type)({obj_id_name: obj['id']})
        update_database_object_from_api(
            obj=api_object,
            object_type=object_type,
            where_data=obj
        )


def get_new_users_from_handles(api):
    handles = database.select_from_table(
        table_name=twitter_config.account_handle_table_name,
        column_names=['Handle'],
        where_data={'NetworkID': 1}
    )
    handles = [handle[0].lower() for handle in handles]

    existing_users = database.select_from_table(
        table_name=twitter_config.user_table_name,
        column_names=['screen_name'],
        where_data=None
    )
    existing_users = [user[0].lower() for user in existing_users]

    new_handles = list(set(handles).difference(set(existing_users)))

    for count_h, handle in enumerate(new_handles, 0):
        print('Handle #{} of {} - {}'.format(count_h+1, len(new_handles), handle))
        try:
            api_user = api.get_user({'screen_name': handle})
            user_data = api_user.AsDict()
            user_data['GetTweets'] = 1
            user_data['GetUserInfo'] = 1
            insert_database_object_from_api(
                obj=user_data,
                object_type='user'
            )
        except twitter.TwitterError as e:
            print(handle, e.message)


def get_latest_tweets_for_users(api):
    users = database.select_from_table(
        table_name=twitter_config.object_table_map['user'],
        column_names=['screen_name', 'since_id'],
        where_data={'GetTweets': 1},
        as_dict=True
    )

    for count_u, user in enumerate(users, 0):
        print('\nUser #{} of {} - {}'.format(count_u+1, len(users), user))
        user['count']=twitter_config.timeline_count
        api_user_timeline = api.get_user_timeline(user)
        for count_t, tweet in enumerate(api_user_timeline, 0):
            print('\tTweet #{} of {}'.format(count_t+1, len(api_user_timeline)))
            insert_database_object_from_api(
                obj=tweet,
                object_type='tweet'
            )
            insert_tweet_sub_dicts(
                tweet=tweet.AsDict(),
                object_type='hashtags'
            )
            insert_tweet_sub_dicts(
                tweet=tweet.AsDict(),
                object_type='urls'
            )
            insert_tweet_sub_dict_reference(
                tweet=tweet.AsDict(),
                object_type='media_obj',
                object_id='media_id',
                reference_object_type='media'
            )
            insert_tweet_sub_dict_reference(
                tweet=tweet.AsDict(),
                object_type='user',
                object_id='user_id',
                reference_object_type='user_mentions'
            )


def insert_tweet_sub_dicts(tweet, object_type):
    if object_type in tweet.keys():
        objects = tweet[object_type]
        for obj in objects:
            obj['tweet_id'] = tweet['id']
            insert_database_object_from_api(
                obj=obj,
                object_type=object_type
            )


def insert_tweet_sub_dict_reference(tweet, object_type, object_id, reference_object_type):
    if reference_object_type in tweet.keys():
        objects = tweet[reference_object_type]
        for obj in objects:
            obj['tweet_id'] = tweet['id']
            insert_database_object_from_api(
                obj=obj,
                object_type=object_type
            )
            reference_object = {
                'tweet_id': tweet['id'],
                object_id: obj['id']
            }
            insert_database_object_from_api(
                obj=reference_object,
                object_type=reference_object_type
            )