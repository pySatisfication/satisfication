# coding=utf-8
import time
import json
import sys
import logging
from redis import StrictRedis
from dateutil import parser

sys.path.append('../')
from ttseries import RedisHashTimeSeries
from ttseries.exceptions import RepeatedValueError,RedisTimeSeriesError

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

REDIS_KEY_VALID_CT = 'all_valid_ct'
REDIS_KEY_DEPTH_PREFIX = 'rt_depth_'
REDIS_KEY_MSCT = 'm_sm_ct'
REDIS_KEY_CT_LIST = 'main_page_ct_lst'
REDIS_KEY_KLINE_PEROID = 'k_{}_{}'

class RedisHandler(object):
    def __init__(self, config_file=None):
        self.logger = logging.getLogger(__name__)

        self._client = StrictRedis(host=REDIS_HOST, 
                                   port=REDIS_PORT, 
                                   decode_responses=True)
        self._t_client = StrictRedis(host=REDIS_HOST, 
                                   port=REDIS_PORT)
        self._tseries = RedisHashTimeSeries(redis_client=self._t_client)

    def set(self, key, value):
        self._client.set(key, value)

    def get(self, key):
        res = self._client.get(key)
        if res:
            #return res.decode('unicode-escape')
            return res
        return None

    def exist_key(self, key):
        return self._client.exists(key)

    def flush(self):
        self._client.flushdb()

    def add_lst(self, series_data):
        if not series_data or not isinstance(timestamps, list):
            self.logger.error("[add]parameter error.")
            return False

        try:
            self._tseries.add_many(key, series_data)
        except RepeatedValueError as e:
            self.logger.error(e)
            return False
        except RedisTimeSeriesError as e:
            self.logger.error(e)
            return False
        except Exception as e:
            self.logger.error(e)
            return False
        return True

    def add(self, key, timestamp, value):
        if not key or not timestamp or not value:
            self.logger.error("[add]parameter is none.")
            return False

        #if not isinstance(timestamps, list):
        #    timestamps = [timestamps]
        #if not isinstance(value, list):
        #    value = [value]

        #if len(timestamps) != len(value):
        #    self.logger.error("[add]length error")
        #    return

        series_data = [(timestamp, value)]
        #for i in range(len(timestamps)):
        #    series_data.append((parser.parse(timestamps[i]).timestamp(), value))

        try:
            self._tseries.add_many(key, series_data)
        except RepeatedValueError as e:
            self.logger.error(e)
            return False
        except RedisTimeSeriesError as e:
            self.logger.error(e)
            return False
        return True

    def get_slice(self, key, s = None, e= None):
        return self._tseries.get_slice(key, start_timestamp=s, end_timestamp=e)

    def delete(self, key, s, e):
        self._tseries.delete(key, start_timestamp=s, end_timestamp=e)

if __name__ == '__main__':
    s0 = time.time()
    rd = RedisHandler()
    e0 = time.time()
    print(e0-s0)

    #print(tseries.count('IC2206'))
    #print(rd.get_slice('IC2206', s = parser.parse('202110181420').timestamp(), e = parser.parse('202110181422').timestamp()))
    #exit(0)

    key = 'test_series'
    #date1 = ['202101012001', '202101012005', '202101012006', '202101012007']
    date1 = '202101012001'
    date2 = '202101011950'
    date3 = '202101012005'
    date4 = '202101012003'
    date5 = '202101012007'

    value = '{"a":11,"b":"2207"}'
    
    #add(tseries, None, None, None)
    s1 = time.time()
    rd.add(key, parser.parse(date5).timestamp(), value)
    #rd.delete(key, parser.parse('197001010000').timestamp(), parser.parse('202101012004').timestamp())
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    e1 = time.time()
    print(e1-s1)
    exit(0)
    
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    rd.add(key, date2, value)
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    rd.add(key, date3, value)
    print(rd.get_slice(key, s = parser.parse('202101011950').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    print(rd.get_slice(key))

    print(rd.get(REDIS_KEY_CT_LIST))
    print(rd.get('rt_depth_m2208'))
    exit(0)
    if not cts:
        pass

    code = 'sc2207'
    val = {"key1":"value","key2":"value2"}
    a = {code:val}
    print(json.dumps(a))
    exit(0)

    # write
    start1 = time.time()
    for ct in cts.split(','):
        rd.set('rt_depth_' + ct, '{"key1":"value","key2":"value2"}')
    end1 = time.time()
    print("t1:", end1-start1)

    start2 = time.time()
    for ct in cts.split(','):
        j = json.loads(rd.get('rt_depth_' + ct))
        print(j["key1"])
    end2 = time.time()
    print("t2:", end2-start2)

    print(rd.get('key_none'))
    exit(0)

    print(rd.set('test_k1', '{"key1":"sd121", "value":9241.11}'))
    print(rd.set('test_k2', '{"key2":"sd122", "value":9241.11}'))

    print(rd.get('test_k1'))
    print(rd.get('test_k2'))

