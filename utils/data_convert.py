import json
from datetime import datetime


def get_start_close_time(data):
    if data:
        start = datetime.strptime(data.split('-')[0], '%H:%M')
        start_time = start.time()
        close = datetime.strptime(data.split('-')[1], '%H:%M')
        close_time = close.time()
        return_list = [start_time.strftime('%H:%M'), close_time.strftime('%H:%M')]
    else:
        return_list = [None, None]
    return return_list


def string_to_dict(dict_string):
    dict_string = dict_string.replace("'", '"')
    return json.loads(dict_string)