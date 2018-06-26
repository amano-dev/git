import re
import psycopg2
import psycopg2.extras
from pytz import timezone
from dateutil import parser
import src.common_functions as cf

"""
Twitter用関数の定義
"""


def search_registered_max_id(cursor, search_word):
    """
    検索キーワードでの登録済み最新レコードのIDを返却
    引数  cursor:DB接続用
          search_word:検索ワード
    戻値  max_id(存在しない場合は-1)
    """
    # SQL作成
    statement = 'SELECT max(id) FROM t_tw_tweets WHERE search_word = %s'
    # バインドセット
    cursor.execute(statement, [search_word])
    # 結果取り出し
    result = cursor.fetchone()
    # 取得済みのツイートのIDをSIDにセット
    if result[0] is not None:
        max_id = result[0]
    # 存在しない場合 -1 返却
    else:
        max_id = -1
    return max_id


def get_next_param(search_tweets):
    """
     次回実行用のパラメータを取得
     パラメータのリストが連結された文字列で返却されるため、
    引数  search_tweets:APIでの検索結果
    戻値  max_id(存在しない場合は-1)
          has_next_result(True/False)
    """
    # next_resultsの存在確認
    if 'next_results' in search_tweets['search_metadata'].keys():
        text_next_parameters = search_tweets['search_metadata']['next_results']

        # 1文字目"?"削除
        text_next_parameters = text_next_parameters[1:]
        # "&"で分割
        next_parameters = text_next_parameters.split('&')
        # 分割したパラメータ分ループし、max_idの検索
        for next_parameter in next_parameters:
            # max_idがあればidと次回実行の判定をセットしループを抜ける
            if re.match("max_id=*", next_parameter):
                max_id = next_parameter[7:]
                has_next_param = True
                return max_id, has_next_param
            else:
                max_id = -1
                has_next_param = False
        # max_idがない場合は次回実行の判定Falseで返却
        return max_id, has_next_param
    # next_resultsが存在しない場合、次回実行の判定をFalseに
    else:
        # 次回実行の判定をFalseに
        max_id = -1
        has_next_param = False
        return max_id, has_next_param


def has_duplicated_tweet(cursor, tweet_id):
    """
    登録済みtweetとの重複チェック
    引数  cursor:DB接続用
          tweet_id:チェック対象のid
    戻値  True(重複あり)/False(重複なし)
    """
    statement = 'SELECT id FROM t_tw_tweets WHERE id = %s'
    cursor.execute(statement, [tweet_id])
    result = cursor.fetchone()
    if result is not None:
        return True
    else:
        return False


def has_duplicated_user(cursor, user_id):
    """
    登録済みuserとの重複チェック
    引数  cursor:DB接続用
          user_id:チェック対象のid
    戻値  True(重複あり)/False(重複なし)
    """
    statement = 'SELECT id FROM m_tw_users WHERE id = %s'
    cursor.execute(statement, [user_id])
    result = cursor.fetchone()
    if result is not None:
        return True
    else:
        return False


def make_register_from_statuses(tweet_statuses):
    """
    APIで取得したデータからDB登録用のdict作成
    引数  tweet_statuses:APIで取得したツイート
          tweet_id:ID
    戻値  tweet_data:t_tw_tweets登録用dict
          place_data:t_tw_tweetplaces登録用dict
          meta_data:t_tw_metadata登録用dict
          user_data:m_tw_users登録用dict
    """
    tweet_data = {}
    place_data = {}
    meta_data = {}
    user_data = {}
    tweet_id = tweet_statuses['id']
    for tweet_status_key in tweet_statuses.keys():
        # find実行用に型変換
        tweet_status_value = str(tweet_statuses[tweet_status_key])

        # 追加情報は登録しない
        if tweet_status_key == 'entities' or tweet_status_key == 'extended_entities':
            continue
        # tweet日時は日本時間に変換
        elif tweet_status_key == 'created_at':
            utc_time = tweet_statuses[tweet_status_key]
            jst_time = parser.parse(utc_time).astimezone(timezone('Asia/Tokyo'))
            tweet_data[tweet_status_key] = jst_time

        # meta情報セット
        elif tweet_status_key == 'metadata':
            meta_data = tweet_statuses[tweet_status_key]
            meta_data['tweet_id'] = tweet_id
            continue

        # 位置情報セット
        elif tweet_status_key == 'place':
            if tweet_statuses[tweet_status_key] is not None:
                place_data = tweet_statuses[tweet_status_key]
                place_data['tweet_id'] = tweet_id
                cf.del_column(place_data, 'id')
                cf.del_column(place_data, 'url')
                cf.del_column(place_data, 'contained_within')
                cf.del_column(place_data, 'bounding_box')
                cf.del_column(place_data, 'attributes')
                continue

        # ユーザ情報セット
        elif tweet_status_key == 'user':
            user_data = tweet_statuses[tweet_status_key]
            #locationが100文字以上あるケースがあったため、切り出し
            user_data['location'] = (user_data['location'])[:99]
            tweet_data['user_id'] = user_data['id']
            cf.del_column(user_data, 'id_str')
            cf.del_column(user_data, 'entities')
            cf.del_column(user_data, 'profile_image_url')
            cf.del_column(user_data, 'profile_banner_url')
            cf.del_column(user_data, 'profile_background_color')
            cf.del_column(user_data, 'profile_background_image_url')
            cf.del_column(user_data, 'profile_background_image_url_https')
            cf.del_column(user_data, 'profile_background_tile')
            cf.del_column(user_data, 'profile_image_url_https')
            cf.del_column(user_data, 'profile_link_color')
            cf.del_column(user_data, 'profile_sidebar_border_color')
            cf.del_column(user_data, 'profile_sidebar_fill_color')
            cf.del_column(user_data, 'profile_text_color')
            cf.del_column(user_data, 'profile_use_background_image')

        # {が見つかった場合、文字列に変換してセット
        elif tweet_status_value.find('{') > -1:
            tweet_data[tweet_status_key] = str(tweet_statuses[tweet_status_key])
        # その他セット
        else:
            tweet_data[tweet_status_key] = tweet_statuses[tweet_status_key]

    return tweet_data, place_data, meta_data, user_data


def insert_tweet_data(cursor, tweet_data):
    """
    t_tw_tweetsへの登録
    引数  cursor:DB接続用
          tweet_data:t_tw_tweets登録用dict
    """
    statement = 'INSERT INTO t_tw_tweets (%s) VALUES %s'
    cursor.execute(statement, (psycopg2.extensions.AsIs(','.join(tweet_data.keys())), tuple(tweet_data.values())))


def insert_place_data(cursor, place_data):
    """
    t_tw_tweetplacesへの登録
    引数  cursor:DB接続用
          place_data:t_tw_tweetplaces登録用dict
    """
    statement = 'INSERT INTO t_tw_tweetplaces (%s) VALUES %s'
    cursor.execute(statement, (psycopg2.extensions.AsIs(','.join(place_data.keys())), tuple(place_data.values())))


def insert_meta_data(cursor, meta_data):
    """
    t_tw_tweetplacesへの登録
    引数  cursor:DB接続用
          meta_data:t_tw_metadata登録用dict
    """
    statement = 'INSERT INTO t_tw_metadata (%s) VALUES %s'
    cursor.execute(statement, (psycopg2.extensions.AsIs(','.join(meta_data.keys())), tuple(meta_data.values())))


def insert_user_data(cursor, user_data):
    """
    m_tw_usersへの登録
    引数  cursor:DB接続用
          user_data:m_tw_users登録用dict
    """
    statement = 'INSERT INTO m_tw_users (%s) VALUES %s'
    cursor.execute(statement, (psycopg2.extensions.AsIs(','.join(user_data.keys())), tuple(user_data.values())))
