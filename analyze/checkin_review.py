import pandas as pd

from config import FILE_DIR
from utils.timer import Timer


def check_review():
    pass


if __name__ == "__main__":
    time = Timer()
    time.start('read csv')
    review_origin = pd.read_csv(FILE_DIR['review']['csv_file_dir'], usecols=['business_id', 'stars', 'date']).dropna()
    time.stop()
