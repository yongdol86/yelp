import pandas as pd
from utils.timer import Timer
import matplotlib.pyplot as plt


desired_width = 320
display_max_columns = 20
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', display_max_columns)


def checkin(df, df2, limit):
    time.start('make df')
    df['cnt'] = df['date'].apply(lambda x: len(x.split(',')))
    df = df[df['cnt'] >= limit]

    df2 = df2[df2['is_open'] > 0]
    df = pd.merge(df, df2, how='left', on='business_id')
    # print(df)
    del df['is_open']
    del df['cnt']

    df = df.assign(date=df['date'].str.split(',')).explode('date')
    df['date'] = df['date'].str.strip()
    df['year'] = df['date'].apply(lambda x: x.split('-')[0])
    df['month'] = df['date'].apply(lambda x: x.split('-')[1])
    time.stop()
    df2 = df[['business_id', 'name', 'city', 'year', 'month']]
    del df
    return df2


def checkin_cnt(start_date, end_date, df, business_id):
    time.start('make df')
    df2 = df[df['business_id'] == business_id]
    title = df2.iloc[1]['name']
    del df
    df2['year-month'] = df2['year'] + '-' + df2['month']
    df3 = df2[(df2['year-month'] >= start_date) & (df2['year-month'] <= end_date)]
    if df3.empty:
        print("this business_id is no data : {}".format(business_id))
    else:
        df4 = pd.DataFrame()
        df4['count'] = df3.groupby(['year-month'])['business_id'].count()
        del df3
        df5 = df4.reset_index()
        del df4
        plt.title(title)
        plt.figure(figsize=(10, 8))
        plt.plot(df5['year-month'], df5['count'], marker='o', markersize=4)
        plt.xticks(rotation=45, horizontalalignment='right')
        plt.xlabel('year-month')
        plt.ylabel('count')
        plt.legend(['checkin-count'], fontsize=12, loc='best')
        plt.show()
        time.stop()
        return df5


if __name__ == "__main__":
    time = Timer()
    time.start('read checkin csv')
    checkin_origin = pd.read_csv('../yelp-datasetcheckin.csv').dropna()
    time.stop()
    time.start('read business csv')
    business_origin = pd.read_csv('../yelp-dataset/business.csv', usecols=['business_id', 'name', 'city', 'is_open'])
    time.stop()

    checkin_cnt_min = 10000
    business_id_list = [
        '-kG0N8sBhBotMbu0KVSPaw',
        # '3kdSl5mo9dWC4clrQjEDGg',
        # 'vHz2RLtfUMVRPFmd7VBEHA'
    ]

    insert_checkin_df = checkin(checkin_origin, business_origin, checkin_cnt_min)

    # each year
    for business_id in business_id_list:
        checkin_cnt('2017-01', '2018-12', insert_checkin_df, business_id)


