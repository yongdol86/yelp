import pandas as pd
from utils.timer import Timer
import matplotlib.pyplot as plt


desired_width = 320
display_max_columns = 20
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', display_max_columns)


def make_review(df):
    # time.start('make df')
    df['year-month'] = df['date'].apply(lambda x: x[:7])
    df['year'] = df['date'].apply(lambda x: x[:4])
    df['month'] = df['date'].apply(lambda x: x[5:7])
    del df['date']
    # time.stop()
    return df


def review_stars_mean(start_date, end_date, business_id, df):
    # time.start('make df')
    df2 = df[df['business_id'] == business_id]
    df3 = df2[(df2['year-month'] >= start_date) & (df2['year-month'] <= end_date)]
    if df3.empty:
        pass
    else:
        df4 = pd.DataFrame()
        df4['stars_mean'] = df3.groupby(['year-month'])['stars'].mean()
        df5 = df4.reset_index()
        plt.title(business_id)
        plt.figure(figsize=(10, 8))
        plt.plot(df5['year-month'], df5['stars_mean'], marker='o', markersize=4)
        plt.xticks(rotation=45, horizontalalignment='right')
        plt.xlabel('year-month')
        plt.ylabel('stars_mean')
        plt.legend(['checkin-count'], fontsize=12, loc='best')
        # plt.show()
        # time.stop()
    return df5


if __name__ == "__main__":
    # time = Timer()
    # time.start('read csv')
    review_origin = pd.read_csv('../yelp-dataset/review.csv', usecols=['business_id', 'stars', 'date']).dropna()
    # time.stop()

    business_id_list = [
        '-kG0N8sBhBotMbu0KVSPaw',
        # 'hihud--QRriCYZw1zZvW4g'
    ]

    insert_df = make_review(review_origin)

    # each year
    for business_id in business_id_list:
        review_stars_mean('2017-01', '2018-12', business_id, insert_df)

