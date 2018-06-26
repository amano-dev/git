import re
import csv
import pandas as pd
import numpy as np
import MeCab
import psycopg2.extras
import config as conf
"""
main
    試作1号
    MeCabと単語感情極性対応表を用いて
    指定した検索ワードでの取得済ツイートに感情値を付与してCSVを吐き出す
"""

# アウトプット用
aura_list = []

# DBコネクション
connection = psycopg2.connect(
    host=conf.DB_HOST,
    database=conf.DB_NAME,
    user=conf.DB_USER,
    password=conf.DB_PASS
)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# DB登録時の検索キーワードで分析を実行する
search_word = conf.DB_SEARCH_WORDS

# SQL実行
statement = 'SELECT id,text FROM t_tw_tweets WHERE search_word = %s'
cursor.execute(statement, [search_word])
results = cursor.fetchall()

# TODO 辞書を最適なものに変えることで精度が上がる？
# 辞書とするテキストファイル読み込み
# 利用テキスト:単語感情極性対応表 http://www.lr.pi.titech.ac.jp/~takamura/pndic_ja.html
pn_df = pd.read_csv('C:\\git\\python_twitter_analyse\\venv\\Lib\\pn_ja.dic.txt',
                    sep=':',
                    encoding='utf-8',
                    names=('Word', 'Reading', 'POS',  'PN'))

# 単語リストと感情極性値(PN)のdict作成
word_list = list(pn_df['Word'])
pn_list = list(pn_df['PN'])  # 中身の型はnumpy.float6
pn_dict = dict(zip(word_list, pn_list))

# 形態素解析用MeCabインスタンス作成
# 茶筌を指定 指定しなければIPA辞書
mecab = MeCab.Tagger ("-Ochasen")

# 取得ツイートごとのループ
for result in results:
    # 形態素解析（改行を含む文字列として得られる）
    parsed = mecab.parse(result[1])
    # 解析結果を1行（1語）ごとに分けてリストにする
    lines = parsed.split('\n')
    # 後ろ2行(EOS ")は不要なので削除
    lines = lines[0:-2]

    # 単語のごとにPNを割り当て
    word_list = []
    for word in lines:
        # 各行をタブとカンマで分割
        l = re.split('\t|,', word)
        # 単語の原型
        base_form = l[2]
        # 辞書と合致した場合PN追加
        if base_form in pn_dict:
            pn = float(pn_dict[base_form])
        # 辞書になかった場合
        else:
            pn = 'notfound'
        # 単語へのPN追加
        d = {'PN': pn, 'Surface': l[0], 'POS': l[3], 'BaseForm': l[2]}
        word_list.append(d)

    # ツイートごとのPN取得
    pn_list = []
    tweet_pn = 0
    for word in word_list:
        pn = word['PN']
        # notfoundだった場合は追加もしない
        if pn != 'notfound':
            pn_list.append(pn)
    # すべて「notfound」じゃなければ平均取得
    if len(pn_list) > 0:
        tweet_pn = np.mean(pn_list)
    # 出力用にテキストから改行を除く
    text = result[1].replace('\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\r\n', ' ')

    # アウトプットリストに格納
    param = {'tweet_id': result[0], 'PN': tweet_pn,  'text': text}
    aura_list.append(param)

# ツイートID、PN値、本文を格納したデータフレームを作成
aura_df = pd.DataFrame(aura_list,
                       columns=['tweet_id', 'PN', 'text']
                       )

# PN値の昇順でソート
aura_df = aura_df.sort_values(by='PN', ascending=True)

# CSVを出力
aura_df.to_csv('tweet_pn_list.csv',
               index=None,
               encoding='utf-8',
               quoting=csv.QUOTE_NONNUMERIC
               )
