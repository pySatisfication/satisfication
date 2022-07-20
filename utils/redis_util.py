import time
import json
import sys
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

class RedisHandler(object):
    def __init__(self):
        self._client = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
        self._tseries = RedisHashTimeSeries(redis_client=self._client)

    def set(self, key, value):
        self._client.set(key, value)

    def get(self, key):
        res = self._client.get(key)
        if res:
            return res.decode('unicode-escape')
        return None

    def exist_key(self, key):
        return self._client.exists(key)

    def flush(self):
        self._client.flushdb()

    def add(self, key, timestamps, value):
        if key is None or timestamps is None or value is None:
            raise ValueError("Input parameter cannot be empty.")

        if not isinstance(timestamps, list):
            timestamps = [timestamps]
        if not isinstance(value, list):
            value = [value]

        assert len(timestamps) == len(value)
        series_data = []
        for i in range(len(timestamps)):
            series_data.append((parser.parse(timestamps[i]).timestamp(), value))

        try:
            self._tseries.add_many(key, series_data)
        except RepeatedValueError as e:
            print(e)
            return False
        except RedisTimeSeriesError as e:
            print(e)
            raise RedisTimeSeriesError('save error')
        return True

    def get_slice(self, key, s = None, e= None):
        return self._tseries.get_slice(key, start_timestamp=s, end_timestamp=e)

    def delete(self, key, s, e):
        self._tseries.delete(key, start_timestamp=s, end_timestamp=e)

if __name__ == '__main__':
    rd = RedisHandler()

    cts = rd.get(REDIS_KEY_CT_LIST)
    print(cts)
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

    #print(tseries.count('IC2206'))
    print(rd.get_slice('IC2206', s = parser.parse('202110181420').timestamp(), e = parser.parse('202110181422').timestamp()))
    exit(0)

    rd.flush()

    key = 'code_3'
    #date1 = ['202101012001', '202101012005', '202101012006', '202101012007']
    date1 = ['202101012001']
    date2 = ['202101011950']
    date3 = ['202101012300']

    value = '{"a":11,"b":"22"}'
    
    #add(tseries, None, None, None)
    #exit(0)
    rd.add(key, date1, value)
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    rd.delete(key, parser.parse('202101012001').timestamp(), parser.parse('202101012002').timestamp())
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    rd.add(key, date2, value)
    print(rd.get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    rd.add(key, date3, value)
    print(rd.get_slice(key, s = parser.parse('202101011950').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    print(rd.get_slice(key))

