import time
import pymysql
import sys
import datetime
import traceback
import logging

sys.path.append("..")
from depth_server.kline import KLine

#lock = threading.Lock()
DB_HOST = 'localhost'
DB_PORT = 3306
USRE_NAME = 'root'
USER_PASSWD = 'p12345'
DB_NAME = 'db_qt_0703'
DEFAULT_CHARSET = 'utf8'
KLINE_TABLE = 'kline_test'
CONN_TIMEOUT = 5

class DBHandler(object):
    def __init__(self, config_file=None):
        self.get_db_conn()
        self.logger = logging.getLogger(__name__)

    def get_db_conn(self):
        self.conn = pymysql.connect(host=DB_HOST,
                                    user=USRE_NAME,
                                    passwd=USER_PASSWD,
                                    db=DB_NAME,
                                    port=DB_PORT,
                                    connect_timeout=CONN_TIMEOUT,
                                    charset=DEFAULT_CHARSET)
        #self.cursor = self.conn.cursor()

    def _reconnect(self):
        try:
            self.conn.ping()
        except:
            self.logger.info('[_reconnect]ping error, reconnecting...')
            print('[_reconnect]ping error, reconnecting...')
            self.get_db_conn()

    def insert_one(self, k_data):
        try:
            self._reconnect()
            cursor = self.conn.cursor()

            sql = "insert into " \
                  "kline(code,period_type,k_time,open,high,low,close,volume,open_interest,turnover,gen_date_time) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % KLINE_TABLE
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
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()

if __name__ == '__main__':
    db_handler = DBHandler()

    time.sleep(5)
    print("conn:", db_handler.conn)

    #kline = KLine(['test_code', 'period', '20220601 10:00:00', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    #db_handler.insert_one(kline)


