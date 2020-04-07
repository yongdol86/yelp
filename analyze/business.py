import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from utils.timer import Timer

time = Timer()

# time.start('read csv')
# time.stop()
# review = pd.read_csv('../yelp-dataset/review.csv')
# tip = pd.read_csv('../yelp-dataset/tip.csv')
# user = pd.read_csv('../yelp-dataset/user.csv')


def business_checkin_cnt():

    time.start()
    business = pd.read_csv('/Users/yongjoolim/Project/yelp/yelp-dataset/business.csv', usecols=['business_id', 'city', 'is_open'])
    checkin = pd.read_csv('/Users/yongjoolim/Project/yelp/yelp-dataset/checkin.csv').dropna().reset_index()
    time.stop()

    checkin['checkin_cnt'] = checkin['date'].apply(lambda x: len(x.split(',')))

    df = pd.merge(business, checkin, how='left', on='business_id')
    df = df[df['is_open'] == 1]
    df = df.dropna().reset_index()
    del df['index']
    del df['date']
    del df['is_open']
    print(df)



def stars_distribution():

    x = business['stars'].value_counts()
    x = x.sort_index()

    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x.index, x.values, alpha=0.8)
    plt.title("stars rating distribution")
    plt.ylabel("business", fontsize=12)
    plt.xlabel("stars", fontsize=12)

    rects = ax.patches
    labels = x.values
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2, height + 5, label, ha='center', va='bottom')

    plt.show()


def category_distribution():
    business_cats = ' '.join(business['categories'].fillna(''))

    cats = pd.DataFrame(business_cats.split(','), columns=['category'])
    x = cats.category.value_counts()
    print("There are ", len(x), " different types/categories of Businesses in Yelp!")
    # prep for chart
    x = x.sort_values(ascending=False)
    x = x.iloc[0:5]

    # chart
    plt.figure(figsize=(16, 4))
    ax = sns.barplot(x.index, x.values, alpha=0.8)  # ,color=color[5])
    plt.title("What are the top categories?", fontsize=25)
    locs, labels = plt.xticks()
    plt.setp(labels, rotation=80)
    plt.ylabel('# businesses', fontsize=12)
    plt.xlabel('Category', fontsize=12)

    # adding the text labels
    rects = ax.patches
    labels = x.values
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label, ha='center', va='bottom')

    plt.show()


def city_distribution():
    x = business['city'].value_counts()
    x = x.sort_values(ascending=False)
    x = x.iloc[0:10]
    plt.figure(figsize=(16, 5))
    ax = sns.barplot(x.index, x.values, alpha=0.8)
    plt.title("Which city has the most reviews?")
    locs, labels = plt.xticks()
    plt.setp(labels, rotation=45)
    plt.ylabel('# businesses', fontsize=12)
    plt.xlabel('City', fontsize=12)

    # adding the text labels
    rects = ax.patches
    labels = x.values
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label, ha='center', va='bottom')

    plt.show()


# if __name__ == "__main__":
    # stars_distribution()
    # category_distribution()
    # city_distribution()
    # business_checkin()
