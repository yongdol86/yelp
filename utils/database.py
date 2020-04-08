import pymysql
from sqlalchemy import create_engine
from config import DATABASES


def pymysql_connect(server, **kwargs):
    con = pymysql.connect(
            database=DATABASES[server]['NAME'],
            host=DATABASES[server]['HOST'],
            user=DATABASES[server]['USER'],
            password=DATABASES[server]['PASSWORD'],
            charset='utf8mb4',
            autocommit=False,
            **kwargs,
        )
    return con


def insert_alchemy(df, server, table_name, method):
    DB_URL = f"mysql+pymysql://{DATABASES[server]['USER']}:{DATABASES[server]['PASSWORD']}@{DATABASES[server]['HOST']}:{DATABASES[server]['PORT']}/{DATABASES[server]['NAME']}?charset=utf8"
    engine = create_engine(DB_URL, encoding='utf-8')
    conn = engine.connect()
    df.to_sql(name=table_name, con=engine, if_exists=method, index=False)
    conn.close()
