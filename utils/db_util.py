import time
import pymysql
import sys

sys.path.append("..")
from kline import KLine

# 数据库连接信息
conn = pymysql.connect(
    host='localhost',
    user='root',
    passwd='p12345',
    db='db_qt_0703',
    port=3306,
    charset="utf8")

def insert_one(k_data):
    cursor = conn.cursor()

    sql = "insert into " \
          "kline(code,period_type,k_time,open,high,low,close,volume,open_interest,turnover,gen_date_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    params = (k_data.code,k_data.period_type,k_data.k_time,
              k_data.open,k_data.high,k_data.low,k_data.close,
              k_data.volume,k_data.open_interest,k_data.turnover,time.time())
    cursor.execute(sql, params)
    conn.commit()

if __name__ == '__main__':


