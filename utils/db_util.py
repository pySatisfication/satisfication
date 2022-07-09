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
#CONN_TIMEOUT = 10

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
                                    #connect_timeout=CONN_TIMEOUT,
                                    charset=DEFAULT_CHARSET)
        #self.cursor = self.conn.cursor()

    def _check_conn(self):
        try:
            if hasattr(self.conn, 'get_sock'):
                sock = self.get_sock_obj()
                if not sock:
                    self.logger.info('[_reconnect]server disconnected, reconnecting...')
                    print('[_reconnect]server disconnected, reconnecting...')
                    self.conn.ping()
            else:
                self.conn.ping()
        except:
            pass

    def insert_one(self, k_data):
        try:
            self._check_conn()
            cursor = self.conn.cursor()

            sql = "insert into " \
                  "{}(code,period_type,k_time,open,high,low,close,volume,open_interest,turnover,gen_date_time) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(KLINE_TABLE)
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

    def close(self):
        self.conn.close()

    def get_sock_obj(self):
        return self.conn.get_sock()

if __name__ == '__main__':
    kline = KLine(['test_code','period','20220601 10:00:00',0.0,0.0,0.0,0.0,0.0,0.0,0.0])
    db_handler = DBHandler()
    db_handler.insert_one(kline)
    print('close...')
    db_handler.close()
    db_handler.insert_one(kline)


