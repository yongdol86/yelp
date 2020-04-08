import json
import pandas as pd
import pymysql

from config import FILE_DIR
from utils.database import pymysql_connect
from utils.timer import Timer


# 원본 데이터 파일을 원하는 파일형식으로 바꾸는 함수(json or csv)
def make_ouput_file(to_type, file):
    file_type = file.split('/')[2].split('.')
    output_file = "../yelp-dataset/{}.output".format(file_type)
    data = [json.loads(line) for line in open(file, encoding='utf8')]
    df = pd.DataFrame(data)
    if to_type == 'json':
        df.to_json(output_file, orient='records', indent=False)
    if to_type == 'csv':
        df.to_csv(output_file, mode='w', index=False)
    return output_file

# 바뀐 파일을 입력받아 line by line 처리, chunk_size 만큼 list 에 넣고 pymysql connector 를 사용하여 데이터 적재
# ex) {"business_id":"--U98MNlDym2cLn36BBPgQ","date":"2011-10-05 22:50:41, 2012-04-11 00:06:36, 2012-07-17 23:55:20"}
# 'date' 컬럼을 ',' 로 split 하여 row 로 입력(위 데이터 입력시 DB에는 3개의 row 로 적재)
def insert_checkin_mysql(json_file, chunk_size):
    with open(json_file, encoding='utf-8') as json_file:
        json_data = json.load(json_file)

    checkin_insert_list = []

    cnx = pymysql_connect('mysql')
    cur = cnx.cursor(pymysql.cursors.DictCursor)
    for line in json_data:
        for date in line['date'].split(','):
            datetime = date.strip()
            checkin_data_dict = dict(
                business_id=line['business_id'],
                date=datetime
            )
            checkin_insert_list.append(checkin_data_dict)

        if len(checkin_insert_list) > chunk_size:
            insert_chekin_sql = """INSERT INTO checkin (`business_id`, `date`) VALUES (%(business_id)s, %(date)s)"""
            time.start('insert {} lines'.format(len(checkin_insert_list)))
            cur.executemany(insert_chekin_sql, checkin_insert_list)
            cnx.commit()
            time.stop()
            checkin_insert_list = []

        else:
            pass
    cur.close()
    cnx.close()


if __name__ == "__main__":

    time = Timer()
    json_file = FILE_DIR['checkin']['file_dir']
    output_file = FILE_DIR['checkin']['ouput_file_dir']

    print("file : ", json_file)
    print("output_file : ", output_file)

    time.start('make json file')
    make_ouput_file(json_file, output_file)
    time.stop()

    chunk_size = 100000
    insert_checkin_mysql(output_file, chunk_size)

