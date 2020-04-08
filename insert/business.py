import pandas as pd

from config import FILE_DIR
from utils.data_convert import string_to_dict, get_start_close_time
from utils.database import insert_alchemy
from utils.timer import Timer


desired_width = 320
display_max_columns = 20
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', display_max_columns)


def daily_opening_hours(json_hours_data):
    monday_open = get_start_close_time(json_hours_data.get('Monday'))[0]
    monday_close = get_start_close_time(json_hours_data.get('Monday'))[1]
    tuesday_open = get_start_close_time(json_hours_data.get('Tuesday'))[0]
    tuesday_close = get_start_close_time(json_hours_data.get('Tuesday'))[1]
    wednesday_open = get_start_close_time(json_hours_data.get('Wednesday'))[0]
    wednesday_close = get_start_close_time(json_hours_data.get('Wednesday'))[1]
    thursday_open = get_start_close_time(json_hours_data.get('Thursday'))[0]
    thursday_close = get_start_close_time(json_hours_data.get('Thursday'))[1]
    friday_open = get_start_close_time(json_hours_data.get('Friday'))[0]
    friday_close = get_start_close_time(json_hours_data.get('Friday'))[1]
    saturday_open = get_start_close_time(json_hours_data.get('Saturday'))[0]
    saturday_close = get_start_close_time(json_hours_data.get('Saturday'))[1]
    sunday_open = get_start_close_time(json_hours_data.get('Sunday'))[0]
    sunday_close = get_start_close_time(json_hours_data.get('Sunday'))[1]

    daily_list = [monday_open,
                  monday_close,
                  tuesday_open,
                  tuesday_close,
                  wednesday_open,
                  wednesday_close,
                  thursday_open,
                  thursday_close,
                  friday_open,
                  friday_close,
                  saturday_open,
                  saturday_close,
                  sunday_open,
                  sunday_close]
    return daily_list


def insert_business(df):
    del df['attributes']
    del df['categories']
    df_hours = df[['business_id', 'hours']].dropna()
    df_hours['hours'] = df_hours['hours'].apply(string_to_dict)
    del df['hours']
    time.start('make hours list')
    df_hours['hours'] = df_hours['hours'].apply(daily_opening_hours)
    time.stop()
    df_hours[['monday_open', 'monday_close',
              'tuesday_open', 'tuesday_close',
              'wednesday_open', 'wednesday_close',
              'thursday_open', 'thursday_close',
              'friday_open', 'friday_close',
              'saturday_open', 'saturday_close',
              'sunday_open', 'sunday_close',
              ]] = pd.DataFrame(df_hours.hours.values.tolist(), index=df_hours.index)

    del df_hours['hours']
    daily_opening_hours(df_hours)
    time.start('insert business_hours')
    insert_alchemy(df_hours, 'mysql', 'business_hours', 'append')
    time.stop()

    time.start('insert business')
    df['hours_id'] = None
    insert_alchemy(df, 'mysql', 'business', 'append')
    time.stop()


if __name__ == "__main__":
    time = Timer()
    csv_file = FILE_DIR['review']['csv_file_dir']

    print("file : ", csv_file)

    time.start('read csv')
    business_df = pd.read_csv(csv_file)
    time.stop()

    insert_business(business_df)
