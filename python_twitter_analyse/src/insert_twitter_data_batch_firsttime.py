from twitter import Twitter, OAuth
import datetime
import psycopg2.extras
from time import sleep
import config as conf
import src.common_functions as cf
import src.twitter_functions as tf
'''
main
    検索ワードリストに基づき、Twitterからつぶやきを取得し、DBに登録する。
    検索期間は過去1週間
'''

# 実行識別用
batch_tag = datetime.datetime.today().strftime('G%Y%m%d%H%M%S')

# 検索ワードのリスト
search_words = conf.TW_SEARCH_WORDS

# 検索実行回数(APIリクエスト制限対策)
count_exec_search = 0

# 初期検索用
search_ymd_datetime = datetime.datetime.today() + datetime.timedelta(days=-1)
s_ymd = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
# 1年前まで検索
serch_end_ymd = datetime.datetime.today() + datetime.timedelta(days=-365)

# Twitter API接続用情報
twitter_obj = Twitter(
    auth=OAuth(
        conf.T_ACCESS_TOKEN,
        conf.T_ACCESS_TOKEN_SECRET,
        conf.T_CONSUMER_KEY,
        conf.T_CONSUMER_SECRET
    )
)

cf.echo_line()
cf.echo('[{0}]ツイート取得バッチ開始'.format(batch_tag))
cf.echo_line()

# 検索ワードリストでのループ
for search_word in search_words:

    # これより過去のツイートを取得
    #m_id = -1
    #取得エラーにより、途中から再開
    m_id = 1009676707127685122
    # 次回実行用パラーメータの保持有無
    has_next_param = True

    # DBコネクション
    connection = psycopg2.connect(
        host=conf.DB_HOST,
        database=conf.DB_NAME,
        user=conf.DB_USER,
        password=conf.DB_PASS
    )
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 同検索ワードでの登録済み最新IDを取得し、検索開始IDとする
    #    s_id = tf.search_registered_max_id(cursor, search_word)
    s_id = -1
    cf.echo('[{0}]でのTweet検索'.format(search_word))

# 日別で検索終了日までループ
    while search_ymd_datetime != serch_end_ymd:
        #search_ymd_datetime_until = search_ymd_datetime
        #search_ymd_datetime = search_ymd_datetime + datetime.timedelta(days=-1)
        # 取得エラーにより、途中から再開
        search_ymd_datetime = search_ymd_datetime + datetime.timedelta(days=-3)
        s_ymd = search_ymd_datetime.strftime('%Y-%m-%d') + '_00:00:00_JST'
        u_ymd = search_ymd_datetime.strftime('%Y-%m-%d') + '_23:59:59_JST'
        cf.echo('[{0}]でのTweet検索'.format(s_ymd))
        has_next_param = True

        # APIで取得できる100件ごとのループ
        while has_next_param:

            # 実行回数を加算
            count_exec_search += 1

            # API制限に引っかかる場合、制限解除までスリープ
            if count_exec_search % conf.T_WSEARCH_LIMIT == 0:
                cf.echo('リクエスト回数制限のため、{}秒スリープ'.format(conf.T_LIMIT_SECONDS))
                sleep(conf.T_LIMIT_SECONDS + 5)

            # Tweet検索
            # TODO 検索結果の時間帯が集中してたり、件数が多い場合取得が途中で止まる(TwitterAPIの仕様のよう)
            #      修正する場合、取得済みの日時をUntilで指定し、それ以前に対してリトライをかけるようにする。
            search_tweets = twitter_obj.search.tweets(
                q=search_word, count=100, since=s_ymd, until=u_ymd, since_id=s_id, max_id=m_id)

            # 取得した件数を表示
            tweet_count = len(search_tweets['statuses'])
            cf.echo('新規ツイートを{}件取得しました'.format(tweet_count))

            # 検索結果から次ループ実行用パラメータをセット
            m_id, has_next_param = tf.get_next_param(search_tweets)

            cf.echo('データベースにツイートを登録します')

            # 登録実行回数
            count_registered_tweet = 1

            # 取得したtweet分ループ
            for tweet_statuses in search_tweets['statuses']:

                tweet_id = tweet_statuses['id']
                tweet_statuses['batch_tag'] = batch_tag
                tweet_statuses['search_word'] = search_word

                # 既に登録しているツイートはスキップ
                if tf.has_duplicated_tweet(cursor, tweet_id):
                    cf.echo('[{0}/{1}] ツイートID:{2}は既に登録済みです'.
                            format(*[count_registered_tweet, tweet_count, tweet_statuses['id']]))
                    count_registered_tweet += 1
                    continue

                # tweet_statusesから登録用dict作成
                tweet_data, place_data, meta_data, user_data = tf.make_register_from_statuses(tweet_statuses)

                # tweetデータの登録
                tf.insert_tweet_data(cursor, tweet_data)
                # metadataの登録
                tf.insert_meta_data(cursor, meta_data)
                # placeデータの登録
                if tweet_statuses['place'] is not None:
                    tf.insert_place_data(cursor, place_data)

                cf.echo('[{0}/{1}] ツイートID:{2}を登録しました'.format(*[count_registered_tweet, tweet_count, tweet_id]))

                # userデータの登録
                user_id = user_data['id']
                # 既に登録しているツイートはスキップ
                if tf.has_duplicated_user(cursor, user_id):
                    cf.echo('[{0}/{1}] ユーザーID:{2}は既に登録済みです'.
                            format(*[count_registered_tweet, tweet_count, user_id]))
                    count_registered_tweet += 1
                    continue
                tf.insert_user_data(cursor, user_data)
                cf.echo('[{0}/{1}] ユーザーID:{2}を登録しました'.format(*[count_registered_tweet, tweet_count, user_id]))

                count_registered_tweet += 1

            # コミット
            connection.commit()
cf.echo_line()
cf.echo('[{0}]ツイート取得バッチ終了'.format(batch_tag))
cf.echo_line()
