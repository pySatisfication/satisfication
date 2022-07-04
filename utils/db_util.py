import time
import pymysql
import sys
import datetime
import threading

sys.path.append("..")
from depth_server.kline import KLine

#lock = threading.Lock()

def insert_one(conn, k_data):
    cursor = conn.cursor()

    sql = "insert into " \
          "kline(code,period_type,k_time,open,high,low,close,volume,open_interest,turnover,gen_date_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    params = (k_data.code,
              k_data.period_type,
              k_data.k_time,
              k_data.open,
              k_data.high,
              k_data.low,
              k_data.close,
              k_data.volume,
              k_data.open_interest,
              k_data.turnover,
              datetime.datetime.now())
    cursor.execute(sql, params)
    conn.commit()

if __name__ == '__main__':
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        passwd='p12345',
        db='db_qt_0703',
        port=3306)
    kline = KLine(['test_code', 'period', '20220601 10:00:00', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    insert_one(conn, kline)


