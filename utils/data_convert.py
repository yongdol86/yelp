import time
from datetime import datetime


def get_start_close_time(data):
    if data:
        start = datetime.strptime(data.split('-')[0], '%H:%M')
        start_time = start.time()
        close = datetime.strptime(data.split('-')[1], '%H:%M')
        close_time = close.time()
        return_list = [start_time, close_time]
    else:
        return_list = [None, None]
    return return_list
