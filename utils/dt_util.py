import datetime

DEFAULT_DT_FORMAT = "%Y%m%d %H:%M:%S"

def dt_from_str(dt_str, dt_format=DEFAULT_DT_FORMAT):
    assert dt_str is not None and len(dt_str.strip()) > 0
    return datetime.datetime.strptime(dt_str, DEFAULT_DT_FORMAT)

def str_from_dt(dt, dt_format=DEFAULT_DT_FORMAT):
    assert isinstance(dt, datetime.datetime)
    return dt.strftime(DEFAULT_DT_FORMAT)

def time_from_dt(dt, dt_format=DEFAULT_DT_FORMAT):
    assert isinstance(dt, datetime.datetime)
    dt_str = str_from_dt(dt)
    assert ' ' in dt_str
    return dt_str.split(' ')[1]

def sec_from_str(dt_str) -> int:
    assert dt_str
    return int(dt_str[-2:])

def min_from_str(dt_str) -> int:
    assert ':' in dt_str
    return int(dt_str.split(':')[1])

def hour_from_str(dt_str) -> int:
    assert ':' in dt_str and ' ' in dt_str
    return int(dt_str.split(' ')[1].split(':')[0])

def detail_from_str(dt_str):
    assert ':' in dt_str and ' ' in dt_str
    dt =  datetime.datetime.strptime(dt_str, DEFAULT_DT_FORMAT)
    return dt.second, dt.minute, dt.hour, dt.day, dt.month, dt.year

def str_add_day1(dt_str):
    dt = dt_from_str(dt_str)
    return str_from_dt(dt + datetime.timedelta(days=1))

def str_sub_min1(dt_str):
    dt = dt_from_str(dt_str)
    return str_from_dt(dt - datetime.timedelta(seconds=60))

