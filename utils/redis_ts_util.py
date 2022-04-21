import sys
from redis import StrictRedis
from dateutil import parser

sys.path.append('../')
from ttseries import RedisHashTimeSeries
from ttseries.exceptions import RepeatedValueError

client = StrictRedis()
tseries = RedisHashTimeSeries(redis_client=client)

def flush():
    client.flushdb()

def add(key, timestamps, value):
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
        tseries.add_many(key, series_data)
    except RepeatedValueError as e:
        print(e)
        return False
    except RedisTimeSeriesError as e:
        print(e)
        raise RedisTimeSeriesError('save error')
    return True

def get_slice(key, s = None, e= None):
    return tseries.get_slice(key, start_timestamp=s, end_timestamp=e)

def delete(key, s, e):
    tseries.delete(key, start_timestamp=s, end_timestamp=e)

if __name__ == '__main__':
    print(tseries.count('IC2206'))
    #print(get_slice('IC2206', s = parser.parse('202110180000').timestamp(), e = parser.parse('202110181422').timestamp()))
    exit(0)

    client.flushdb()

    key = 'code_3'
    #date1 = ['202101012001', '202101012005', '202101012006', '202101012007']
    date1 = ['202101012001']
    date2 = ['202101011950']
    date3 = ['202101012300']

    value = '{"a":11,"b":"22"}'
    
    #add(tseries, None, None, None)
    #exit(0)
    add(key, date1, value)
    print(get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    delete(key, parser.parse('202101012001').timestamp(), parser.parse('202101012002').timestamp())
    print(get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    add(key, date2, value)
    print(get_slice(key, s = parser.parse('202101012000').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    add(key, date3, value)
    print(get_slice(key, s = parser.parse('202101011950').timestamp(), e = parser.parse('202101012008').timestamp()))
    
    print(get_slice(key))

