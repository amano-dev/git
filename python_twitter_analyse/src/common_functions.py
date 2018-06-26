from datetime import datetime
"""
共通関数の定義
"""


def echo(text):
    """
    タイムスタンプ付与でのコンソール出力
    """
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"), text)


def echo_line():
    """
    ライン出力
    """
    print('--------------------------------------------------------------')


def del_column(dect, key):
    """
    dectから使用したカラムを削除
    """
    if key in dect.keys():
        del dect[key]