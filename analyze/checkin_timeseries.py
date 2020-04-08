import pandas as pd

from config import FILE_DIR
from utils.timer import Timer

desired_width = 320
display_max_columns = 20
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', display_max_columns)


def time_series_checkin(df):
    print(df.info())


if __name__ == "__main__":
    time = Timer()
    # time.start('read review csv')
    # review = pd.read_csv(FILE_DIR['review']['csv_file_dir'], usecols=['business_id', 'stars', 'date']).dropna()
    # time.stop()

    time.start('read checkin csv')
    checkin = pd.read_csv(FILE_DIR['checkin']['csv_file_dir']).dropna()
    time.stop()
    time_series_checkin(checkin)
    
