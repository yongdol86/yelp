import json

import pandas as pd

from utils.data_convert import get_start_close_time
from utils.database import RDB
from utils.timer import Timer


def make_json_file(file, output_file):
    data = [json.loads(line) for line in open(file, encoding='utf8')]
    df = pd.DataFrame(data)
    # print("df.head() : ", df.head())
    # df.to_json(output_file, orient='records', indent=False)
    df.to_csv(output_file, mode='w', index=False)
    del df


def insert_table(json_file):
    file_name = json_file.split('\\')[2]
    with open(json_file, encoding='utf-8') as json_file:
        json_data = json.load(json_file)

    checkin_insert_list = []
    tips_insert_list = []
    business_insert_list = []
    hours_insert_list = []
    user_insert_list = []
    review_insert_list = []

    base_date = '2015-01-01'

    if 'checkin' in file_name:
        for line in json_data:
            for date in line['date'].split(','):
                idx_rddate = date.strip().split(' ')[0]
                if idx_rddate > base_date:
                    checkin_data_dict = dict(
                        business_id=line['business_id'],
                        rddate=date,
                        idx_rddate=idx_rddate
                    )
                    checkin_insert_list.append(checkin_data_dict)
                else:
                    pass

        insert_chekin_sql = """INSERT INTO checkin (`business_id`, `rddate`, `idx_rddate`) VALUES (%(business_id)s, %(rddate)s, %(idx_rddate)s)"""
        target_db.execute(insert_chekin_sql, checkin_insert_list)

    if 'tip' in file_name:
        for line in json_data:
            # text_one_space = " ".join(line['text'].split())
            # text = text_one_space.rstrip('\n')
            idx_rddate = line['date'].split(' ')[0]
            if idx_rddate > base_date:
                tips_data_dict = dict(
                    user_id=line['user_id'],
                    business_id=line['business_id'],
                    text=line['text'],
                    rddate=line['date'],
                    compliment_count=line['compliment_count'],
                    idx_rddate=idx_rddate
                )
                tips_insert_list.append(tips_data_dict)
            else:
                pass
        insert_tips_sql = """INSERT INTO tip (`user_id`, `business_id`, `text`, `rddate`, `compliment_count`, `idx_rddate`) VALUES (%(user_id)s, %(business_id)s, %(text)s, %(rddate)s, %(compliment_count)s, %(idx_rddate)s)"""
        target_db.execute(insert_tips_sql, tips_insert_list)

    if 'business' in file_name:
        for line in json_data:
            business_dict = dict(
                business_id=line['business_id'],
                name=line['name'],
                address=line['address'],
                city=line['city'],
                state=line['state'],
                postal_code=line['postal_code'],
                latitude=line['latitude'],
                longitude=line['longitude'],
                stars=line['stars'],
                review_count=line['review_count'],
                is_open=line['is_open'],
                # attributes_id=None,
                # category_id=None,
                # hours_id=None
            )
            business_insert_list.append(business_dict)

            hours = line.get('hours')
            if hours:
                monday = hours.get('Monday')
                tuesday = hours.get('Tuesday')
                wednesday = hours.get('Wednesday')
                thursday = hours.get('Thursday')
                friday = hours.get('Friday')
                saturday = hours.get('Saturday')
                sunday = hours.get('Sunday')

                hours_dict = dict(
                    business_id=line['business_id'],
                    monday_open=get_start_close_time(monday)[0],
                    monday_close=get_start_close_time(monday)[1],
                    tuesday_open=get_start_close_time(tuesday)[0],
                    tuesday_close=get_start_close_time(tuesday)[1],
                    wednesday_open=get_start_close_time(wednesday)[0],
                    wednesday_close=get_start_close_time(wednesday)[1],
                    thursday_open=get_start_close_time(thursday)[0],
                    thursday_close=get_start_close_time(thursday)[1],
                    friday_open=get_start_close_time(friday)[0],
                    friday_close=get_start_close_time(friday)[1],
                    saturday_open=get_start_close_time(saturday)[0],
                    saturday_close=get_start_close_time(saturday)[1],
                    sunday_open=get_start_close_time(sunday)[0],
                    sunday_close=get_start_close_time(sunday)[1]
                )
                hours_insert_list.append(hours_dict)
            else:
                hours_dict = dict(
                    business_id=line['business_id'],
                    monday_open=None,
                    monday_close=None,
                    tuesday_open=None,
                    tuesday_close=None,
                    wednesday_open=None,
                    wednesday_close=None,
                    thursday_open=None,
                    thursday_close=None,
                    friday_open=None,
                    friday_close=None,
                    saturday_open=None,
                    saturday_close=None,
                    sunday_open=None,
                    sunday_close=None
                )
                hours_insert_list.append(hours_dict)
        insert_business_sql = """INSERT INTO business (`business_id`, `name`, `address`, `city`, `state`, `postal_code`, `latitude`, `longitude`, `stars`, `review_count`, `is_open`)
                                                VALUES (%(business_id)s, %(name)s, %(address)s, %(city)s, %(state)s, %(postal_code)s, %(latitude)s, %(longitude)s, %(stars)s, %(review_count)s, %(is_open)s)"""
        target_db.execute(insert_business_sql, business_insert_list)

        insert_hours_sql = """INSERT INTO business_hours (`business_id`, `monday_open`, `monday_close`, `tuesday_open`, `tuesday_close`, `wednesday_open`, `wednesday_close`, `thursday_open`, `thursday_close`, `friday_open`, `friday_close`, `saturday_open`, `saturday_close`, `sunday_open`, `sunday_close`) 
                                         VALUES (%(business_id)s, %(monday_open)s, %(monday_close)s, %(tuesday_open)s, %(tuesday_close)s, %(wednesday_open)s, %(wednesday_close)s, %(thursday_open)s, %(thursday_close)s, %(friday_open)s, %(friday_close)s, %(saturday_open)s, %(saturday_close)s, %(sunday_open)s, %(sunday_close)s)"""
        target_db.execute(insert_hours_sql, hours_insert_list)

    if 'user' in file_name:
        for line in json_data:
            elite_list_len = None
            friends_list_len = None
            if len(line.get('elite')) > 0:
                elite_list_len = len([elite for elite in line['elite'].split(',')])
            if len(line.get('friends')) > 0:
                friends_list_len = len([friend for friend in line['friends'].split(',')])

            user_dict = dict(
                user_id=line['user_id'],
                name=line['name'],
                review_count=line['review_count'],
                yelping_since=line['yelping_since'],
                useful=line['useful'],
                funny=line['funny'],
                cool=line['cool'],
                elite=elite_list_len,
                friends=friends_list_len,
                fans=line['fans'],
                average_stars=line['average_stars'],
                compliment_hot=line['compliment_hot'],
                compliment_more=line['compliment_more'],
                compliment_profile=line['compliment_profile'],
                compliment_cute=line['compliment_cute'],
                compliment_list=line['compliment_list'],
                compliment_note=line['compliment_note'],
                compliment_plain=line['compliment_plain'],
                compliment_cool=line['compliment_cool'],
                compliment_funny=line['compliment_funny'],
                compliment_writer=line['compliment_writer'],
                compliment_photos=line['compliment_photos']
            )
            user_insert_list.append(user_dict)
        insert_user_sql = """INSERT INTO user (`user_id`, `name`, `review_count`, `yelping_since`, `useful`, `funny`, `cool`, `elite`, `friends`, `fans`, `average_stars`, `compliment_hot`, `compliment_more`, `compliment_profile`, `compliment_cute`, `compliment_list`, `compliment_note`, `compliment_plain`, `compliment_cool`, `compliment_funny`, `compliment_writer`, `compliment_photos`)
                                        VALUES (%(user_id)s, %(name)s, %(review_count)s, %(yelping_since)s, %(useful)s, %(funny)s, %(cool)s, %(elite)s, %(friends)s, %(fans)s, %(average_stars)s, %(compliment_hot)s, %(compliment_more)s, %(compliment_profile)s, %(compliment_cute)s, %(compliment_list)s, %(compliment_note)s, %(compliment_plain)s, %(compliment_cool)s, %(compliment_funny)s, %(compliment_writer)s, %(compliment_photos)s)"""
        target_db.execute(insert_user_sql, user_insert_list)

    if 'review' in file_name:
        for line in json_data:
            idx_rddate = line['date'].split(' ')[0]
            if idx_rddate > base_date:
                review_data_dict = dict(
                    review_id=line['review_id'],
                    user_id=line['user_id'],
                    business_id=line['business_id'],
                    stars=line['stars'],
                    useful=line['useful'],
                    funny=line['funny'],
                    cool=line['cool'],
                    text=line['text'],
                    rddate=line['date'],
                    idx_rddate=idx_rddate
                )
                review_insert_list.append(review_data_dict)
            else:
                pass
        insert_review_sql = """INSERT INTO review (`review_id`, `user_id`, `business_id`, `stars`, `useful`, `funny`, `cool`, `text`, `rddate`, `idx_rddate`) 
                            VALUES (%(review_id)s, %(user_id)s, %(business_id)s, %(stars)s, %(useful)s, %(funny)s, %(cool)s, %(text)s, %(rddate)s, %(idx_rddate)s)"""
        target_db.execute(insert_review_sql, review_insert_list)


if __name__ == "__main__":
    input_file_list = [
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_00",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_01",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_02",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_03",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_04",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_05",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_06",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_07",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_08",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_09",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_10",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_11",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_12",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_13",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_14",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_15",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_16",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\checkin_17",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_00",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_01",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_02",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_03",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_04",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_05",
        # "D:\\yanolza\yelp_academic_dataset_tip\\tip_06",
        # "D:\\yanolza\yelp_academic_dataset_business\\business_00",
        # "D:\\yanolza\yelp_academic_dataset_business\\business_01",
        # "D:\\yanolza\yelp_academic_dataset_business\\business_02"
        # "D:\\yanolza\\yelp_academic_dataset_business\\yelp_academic_dataset_business.json",
        # "D:\\yanolza\\yelp_academic_dataset_checkin\\yelp_academic_dataset_checkin.json",
        # "D:\\yanolza\\yelp_academic_dataset_tip\\yelp_academic_dataset_tip.json",
        # "D:\\yanolza\\yelp_academic_dataset_review\\yelp_academic_dataset_review.json",
        # "D:\\yanolza\\yelp_academic_dataset_user\\yelp_academic_dataset_user.json",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_00",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_01",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_02",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_03",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_04",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_05",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_06",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_07",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_08",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_09",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_10",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_11",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_12",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_13",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_14",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_15",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_16",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_17",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_18",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_19",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_20",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_21",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_22",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_23",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_24",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_25",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_26",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_27",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_28",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_29",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_30",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_31",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_32",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_33",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_34",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_35",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_36",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_37",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_38",
        # "D:\\yanolza\\yelp_academic_dataset_user\\user_39",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_00",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_01",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_02",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_03",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_04",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_05",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_06",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_07",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_08",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_09",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_10",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_11",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_12",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_13",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_14",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_15",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_16",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_17",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_18",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_19",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_20",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_21",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_22",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_23",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_24",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_25",
        # "D:\\yanolza\\yelp_academic_dataset_review\\review_26",
        "/Users/yongjoolim/Project/yelp/yelp-dataset/yelp_academic_dataset_tip.json",
        "/Users/yongjoolim/Project/yelp/yelp-dataset/yelp_academic_dataset_user.json"
    ]
    time = Timer()
    # target_db = RDB('local_whyzoo')
    for file in input_file_list:
        # file_type = file.split('\\')[3].split('_')[0]
        file_type = file.split('/')[6].split('.')[0].split('_')[3]

        print("file_type : ", file_type)
        # output_file = "D:\\yanolza\\yelp_academic_dataset_{}\\{}.csv".format(file_type, file.split('\\')[3])
        output_file = "/Users/yongjoolim/Project/yelp/yelp-dataset/{}.csv".format(file_type)
        print("file : ", file)
        print("output_file : ", output_file)
        make_json_file(file, output_file)
        time.start()
        # insert_table(output_file)
        time.stop()
