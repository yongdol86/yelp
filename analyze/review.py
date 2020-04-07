import copy

import pandas as pd
from sqlalchemy import create_engine

from utils.database import RDB
from utils.timer import Timer
import matplotlib.pyplot as plt
import seaborn as sns


desired_width = 320
display_max_columns = 20
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', display_max_columns)


def review(df):
    time.start('make df')
    df['year-month'] = df['date'].apply(lambda x: x[:7])
    df['year'] = df['date'].apply(lambda x: x[:4])
    df['month'] = df['date'].apply(lambda x: x[5:7])
    del df['date']
    time.stop()
    return df


def review_stars_mean(start_date, end_date, business_id, df):
    time.start('make df')
    df2 = df[df['business_id'] == business_id]
    df3 = df2[(df2['year-month'] >= start_date) & (df2['year-month'] <= end_date)]
    if df3.empty:
        pass
    else:
        df4 = pd.DataFrame()
        df4['stars_mean'] = df3.groupby(['year-month'])['stars'].mean()
        df5 = df4.reset_index()
    time.stop()
    return df5


def insert_mysql(df):
    # sql alchemy
    engine = create_engine("mysql+pymysql://root:"+"dydehf"+"@127.0.0.1:3306/yelp?charset=utf8", encoding='utf-8')
    conn = engine.connect()
    time.start('insert mysql')
    df.to_sql(name='checkin_date_year_month', con=engine, if_exists='append', index=False)
    time.stop()
    conn.close()


if __name__ == "__main__":
    time = Timer()
    time.start('read csv')
    review_origin = pd.read_csv('../yelp-dataset/review.csv', usecols=['business_id', 'stars', 'date']).dropna()
    time.stop()

    business_id_list = [
        '-kG0N8sBhBotMbu0KVSPaw',
        # 'hihud--QRriCYZw1zZvW4g'
    ]

    insert_df = review(review_origin)
    # insert_mysql(insert_df)

    # each year
    for business_id in business_id_list:
        # review_stars_mean('year', insert_df, business_id)
        # review_stars_mean('month', insert_df, business_id)
        review_stars_mean('2017-01', '2018-12', business_id, insert_df)

