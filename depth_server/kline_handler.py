import time
import sys
import json
import logging
import logging.config
import threading
import traceback
import datetime
import argparse

sys.path.append("..")
from utils import dt_util
from depth import Depth
from tran_time_helper import *

if sys.version > '3':
    import queue as Queue
else:
    import Queue

import parallel as px
from kafka import KafkaConsumer,KafkaProducer

# logger
config_file = 'logger_config.json'
with open(config_file, 'r', encoding='utf-8') as file:
    logging.config.dictConfig(json.load(file))
logger = logging.getLogger("worker")

MESSAGE_SOURCE = ['kafka','redis','rabbitmq']
HACK_DELAY = 0.02
NUM_HANDLER = 10
QUEUE_SIZE = 1000000
queues = [Queue.Queue(QUEUE_SIZE) for i in range(NUM_HANDLER)]

FUTURES_DEPTH_TOPIC = 'FuturesDepthData'
FUTURES_KLINE_TPOIC = 'FuturesKLineTest'

GLOBAL_CACHE_KEY = 'global_cache'
KEY_K_15S = 'key_k_15s'
KEY_K_30S = 'key_k_30s'
KEY_K_1M  = 'key_k_1m'
KEY_K_3M  = 'key_k_3m'
KEY_K_5M  = 'key_k_5m'
KEY_K_15M = 'key_k_15m'
KEY_K_30M = 'key_k_30m'
KEY_K_1H  = 'key_k_1h'
KEY_K_2H  = 'key_k_2h'
KEY_K_1D  = 'key_k_1d'
M_PERIOD_KEY = [KEY_K_15S, KEY_K_30S,
                KEY_K_1M, KEY_K_3M, KEY_K_5M, KEY_K_15M, KEY_K_30M,
                KEY_K_1H, KEY_K_2H, KEY_K_1D]

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='arguments for modular call')
    parser.add_argument('--depth_source', dest='depth_source', help='transaction data file',
                        default='../data/depth_data.csv', type=str)
    #parser.add_argument('--target_file_name', dest='target_file_post', help='transaction data file',
    #                    default='', type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

class KCache(object):
    def __init__(self, *args):
        self.code = args[0]
        dt_str = args[1]
        dt = dt_util.dt_from_str(dt_str)

        self.open_dt = dt
        self.open_dt_str = dt_str
        self.end_dt = dt
        self.end_dt_str = dt_str
        self.end_sec = dt_util.sec_from_str(dt_str)

        self.open = args[2] if len(args) >= 9 else 0.0
        self.high = args[3] if len(args) >= 9 else 0.0
        self.low = args[4] if len(args) >= 9 else 0.0
        self.close = args[5] if len(args) >= 9 else 0.0

        self._volume = args[6] if len(args) >= 9 else 0.0
        self._open_interest = args[7] if len(args) >= 9 else 0.0
        self._turnover = args[8] if len(args) >= 9 else 0.0

    def __repr__(self):
        if hasattr(self, 'open'):
            return "{},{},{},{},{},{},{},{},{},{}".format(
                self.code, self.open_dt_str, self.end_dt_str, 
                self.open, self.high, self.low, self.close, 
                self._volume, self._open_interest, self._turnover)
        else:
            return "{},{},{},{},{},{},{},{},{},{}".format(
                self.code, self.open_dt_str, self.end_dt_str)

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value):
        self._volume = value

    @property
    def open_interest(self):
        return self._open_interest

    @open_interest.setter
    def open_interest(self, value):
        self._open_interest = value

    @property
    def turnover(self):
        return self._turnover

    @turnover.setter
    def turnover(self, value):
        self._turnover = value

class KLine(object):
    def __init__(self, code, period, date_time, 
            open_price, high_price, low_price, close_price, 
            volume, open_interest, turnover):
        self.code = code
        self.period_type = period
        self.k_time = date_time
        self.open = open_price
        self.high = high_price
        self.low = low_price
        self.close = close_price
        self.volume = volume
        self.open_interest = open_interest
        self.turnover = turnover

    def __repr__(self):
        return "{},{},{},{},{},{},{},{},{},{}".format(
            self.code, self.period_type, self.k_time, 
            self.open, self.high, self.low, self.close, 
            self.volume, self.open_interest, self.turnover)

    def print_line(self):
        return "{},{},{},{},{},{},{},{},{},{}".format(
            self.code, self.period_type, self.k_time, 
            self.open, self.high, self.low, self.close, 
            self.volume, self.open_interest, self.turnover)

class KHandlerThread(threading.Thread):
    """
    Thread that can be canceled using `cancel()`.
    """
    def __init__(self, h_id, event, data_source):
        threading.Thread.__init__(self)
        # 线程名
        self.name = f"handler_{h_id}"
        # 线程ID/分桶ID
        self._hid = h_id
        # 数据源：消息队列 or 文件
        self._data_source = data_source

        # 队列控制
        self._event = event
        self._first_depth = {}

        # 时间处理工具包
        self._tth = TranTimeHelper()
        # 记录临时k线
        self._k_lines = []

        # 记录K线结果
        self._kline_cache = {}
        # 记录上一个depth
        self._last_depth = {}
        # 记录品种竞价时间
        self._code_auction_hour = {}

        # 消息队列
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        # 休市&收盘
        # 线程同步事件
        self._closeout_event = threading.Event()
        # 子线程
        self.close_scaner = threading.Thread(target=self.gen_cloing_kline)
        self.close_scaner.start()

    @property
    def k_lines(self):
        return self._k_lines

    def gen_cloing_kline(self):
        while True:
            self._closeout_event.wait()
            while self._closeout_event.isSet():
                now_dt = datetime.datetime.now()
                now_dt_str = dt_util.str_from_dt(now_dt)    # 20220522 15:15:00
                #now_date = now_dt_str.split(' ')[0]         # 20220522
                now_time = now_dt_str.split(' ')[1]         # 15:15:00

                if now_time not in [TIME_TEN_SIXTEEN, TIME_ELEVEN_THIRTYONE,
                                    TIME_FIFTEEN_ONE, TIME_FIFTEEN_SIXTEEN,
                                    TIME_TWENTYTHREE_ONE,
                                    TIME_ONE_ONE, TIME_TWO_THIRTYONE]:
                    time.sleep(1)
                    continue
                for code, caches in self._kline_cache.items():
                    if caches is None or len(caches) == 0:
                        continue
                    code_prefix = self._tth.get_code_prefix(code)
                    # global cache
                    g_cache = self._kline_cache[code][GLOBAL_CACHE_KEY]

                    end_dt_str = g_cache.end_dt_str
                    cache_date = end_dt_str.split(' ')[0]

                    # 10:15, 11:30, 23:00, 01:00, 02:30, 15:00, 15:15
                    if now_time == TIME_TEN_SIXTEEN:
                        norm_close_dt_str = cache_date + ' ' + TIME_TEN_FIFTEEN
                        #norm_end_dt_str = cache_date + ' ' + TIME_TEN_SIXTEEN
                    elif now_time == TIME_ELEVEN_THIRTYONE:
                        norm_close_dt_str = cache_date + ' ' + TIME_ELEVEN_THIRTY
                        #norm_end_dt_str = cache_date + ' ' + TIME_ELEVEN_THIRTYONE
                    elif now_time == TIME_FIFTEEN_ONE:
                        norm_close_dt_str = cache_date + ' ' + TIME_FIFTEEN
                        #norm_end_dt_str = cache_date + ' ' + TIME_FIFTEEN_ONE
                    elif now_time == TIME_FIFTEEN_SIXTEEN:
                        norm_close_dt_str = cache_date + ' ' + TIME_FIFTEEN_FIFTEEN
                        #norm_end_dt_str = cache_date + ' ' + TIME_FIFTEEN_SIXTEEN
                    elif now_time == TIME_TWENTYTHREE_ONE:
                        norm_close_dt_str = cache_date + ' ' + TIME_TWENTYTHREE
                        #norm_end_dt_str = cache_date + ' ' + TIME_TWENTYTHREE_ONE
                    elif now_time == TIME_ONE_ONE:
                        norm_close_dt_str = cache_date + ' ' + TIME_ONE
                        #norm_end_dt_str = cache_date + ' ' + TIME_ONE_ONE
                    elif now_time == TIME_TWO_THIRTYONE:
                        norm_close_dt_str = cache_date + ' ' + TIME_TWO_THIRTY
                        #norm_end_dt_str = cache_date + ' ' + TIME_TWO_THIRTYONE

                    #if now_time == TIME_FIFTEEN_ONE:
                    #    norm_close_dt_str = cache_date + ' ' + TIME_FIFTEEN
                    #if (now_time == TIME_FIFTEEN_ONE and self._tth.check_close_time(code_prefix, CLOSE_TIME3) and cache.end_dt_str < norm_close_dt_str):
                    #    or (now_time == TIME_FIFTEEN_SIXTEEN and self._tth.check_close_time(code_prefix, CLOSE_TIME4) and cache.end_dt_str < norm_close_dt_str):

                    if (now_time == TIME_TEN_SIXTEEN and self._tth.check_morning_suspend(code_prefix)) \
                            or now_time == TIME_ELEVEN_THIRTYONE \
                            or (now_time == TIME_FIFTEEN_ONE and self._tth.check_close_time(code_prefix, CLOSE_TIME3)) \
                            or (now_time == TIME_FIFTEEN_SIXTEEN and self._tth.check_close_time(code_prefix, CLOSE_TIME4)) \
                            or (now_time == TIME_TWENTYTHREE_ONE and self._tth.check_close_time(code_prefix, CLOSE_TIME5)) \
                            or (now_time == TIME_ONE_ONE and self._tth.check_close_time(code_prefix, CLOSE_TIME6)) \
                            or (now_time == TIME_TWO_THIRTYONE and self._tth.check_close_time(code_prefix, CLOSE_TIME7)):
                        logger.debug('[gen_cloing_kline]generate last K, code:{}, end_time:{}, cur_time:{}'.format(
                            code,
                            end_dt_str,
                            norm_close_dt_str))
                        cur_depth = Depth(norm_close_dt_str.split(' ') + [code])
                        # 使用实际缓存中的最后更新时间，好处是可以处理非正常depth
                        self.depth_tick(cur_depth, close_out=True, mock_end_dt_str=end_dt_str)

                    # 收盘需要清空品种对应全部缓存, 其他休市或停盘时间只是处理完一个周期就清空对应周期的缓存
                    if now_time in [TIME_FIFTEEN_ONE, TIME_FIFTEEN_SIXTEEN]:
                        logger.debug('[gen_cloing_kline]clear cache, code:{}, now_time:{}'.format(code, now_time))
                        self._kline_cache[code] = {}
                        self._last_depth.pop(code)
                        self._code_auction_hour.pop(code)
                self._closeout_event.clear()
            logger.debug('sleeping, wait closeout event being set...')

    def cancel(self):
        self._isCanceled = True

    @property
    def isCanceled(self):
        return self._isCanceled

    def run(self):
        try:
            self._event.wait()

            d_id = 0
            while self._event.isSet():
                # HACK: Use a timeout when getting the item from the queue
                # because between `empty()` and `get()` another consumer might
                # have removed it.
                try:
                    #d_id += 1
                    #if d_id % 1000 == 0:
                    #    print('consume id: %s, processed: %s' % (self._hid, d_id))

                    item = queues[self._hid].get()
                    #print(item.update_time)
                    self.consume(item)
                except Queue.Empty:
                    pass
        except Exception as error:
            logger.warning("cannot continue to consume: %s", error)

            exc_info = sys.exc_info()
            logger.debug("raising notified error: %s %s", exc_info[0], exc_info[1])
            for filename, linenum, funcname, source in traceback.extract_tb(exc_info[2]):
                logger.warning("%-23s:%s '%s' in %s", filename, linenum, source, funcname)

    def check_out_sec(self, sec, trading_day, end_sec, end_dt, cur_dt):
        if end_dt == cur_dt:
            return None
        secs = (int(end_sec / sec) + 1) * sec - end_sec
        kline_end_dt = end_dt + datetime.timedelta(seconds=secs)

        # cur_dt:  action_day + update_time
        # 条件1：0点, 条件2：非0点时间
        if (cur_dt.hour <= 11 and cur_dt.hour >= 0) and (end_dt.hour < 24 and end_dt.hour >= 21) \
                or (cur_dt >= kline_end_dt and end_dt < kline_end_dt):
            tmp_dt_str = dt_util.str_from_dt(kline_end_dt - datetime.timedelta(seconds=sec))
            tmp = tmp_dt_str.split(' ')
            if tmp[0] != trading_day:
                return trading_day + ' ' + tmp[1]
            else:
                return tmp_dt_str
        return None

    def check_out_min(self, minute, trading_day, end_sec, end_min, end_dt, cur_dt):
        if end_dt == cur_dt:
            return None
        secs = (int(end_min / minute) + 1) * minute * 60 - (end_min * 60 + end_sec)
        kline_end_dt = end_dt + datetime.timedelta(seconds=secs)

        # 条件1：0点, 条件2：非0点时间
        if (cur_dt.hour <= 11 and cur_dt.hour >= 0) and (end_dt.hour < 24 and end_dt.hour >= 21) \
                or (cur_dt >= kline_end_dt and end_dt < kline_end_dt):
            tmp_dt_str = dt_util.str_from_dt(kline_end_dt - datetime.timedelta(seconds=minute*60))[:-3]
            tmp = tmp_dt_str.split(' ')
            if tmp[0] != trading_day:
                return trading_day + ' ' + tmp[1]
            else:
              return tmp_dt_str
        return None
            
    def check_out_30m(self, code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str):
        last_update_time = end_dt_str.split(' ')[1]

        # 09:00:00开盘, 上午有中场休息
        if self._tth.check_open_time(code_prefix, OPEN_TIME1) and self._tth.check_morning_suspend(code_prefix):
            is_case6 = self._tth.check_close_time(code_prefix, CLOSE_TIME7)
            is_case5 = self._tth.check_close_time(code_prefix, CLOSE_TIME6)
            is_case4 = self._tth.check_close_time(code_prefix, CLOSE_TIME5)
            is_case3 = self._tth.check_close_time(code_prefix, CLOSE_TIME3)
            # case3: 15:00:00收盘
            # case4: 23:00:00收盘
            # case5: 01:00:00收盘
            # case6: 02:30:00收盘, 15:00:00收盘
            if is_case6 or is_case5 or is_case4 or is_case3:
                # 9:30:00
                if cur_time >= TIME_NINE_THIRTY and last_update_time < TIME_NINE_THIRTY:
                    return trading_day + ' ' + KTIME_NINE
                # 10:00:00
                if cur_time >= TIME_TEN and last_update_time < TIME_TEN:
                    return trading_day + ' ' + KTIME_NINE_THIRTY
                # 10:45:00
                if cur_time >= TIME_TEN_FORTYFIVE and last_update_time < TIME_TEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_TEN
                # 11:15:00
                if cur_time >= TIME_ELEVEN_FIFTEEN and last_update_time < TIME_ELEVEN_FIFTEEN:
                    return trading_day + ' ' + KTIME_TEN_FOURTYFIVE
                # 13:45:00
                if cur_time >= TIME_THIRTEEN_FORTYFIVE and last_update_time < TIME_THIRTEEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_ELEVEN_FIFTEEN
                # 14:15:00
                if cur_time >= TIME_FOURTEEN_FIFTEEN and last_update_time < TIME_FOURTEEN_FIFTEEN:
                    return trading_day + ' ' + KTIME_THIRTEEN_FORTYFIVE
                # 14:45:00
                if cur_time >= TIME_FOURTEEN_FORTYFIVE and last_update_time < TIME_FOURTEEN_FORTYFIVE: 
                    return trading_day + ' ' + KTIME_FOURTEEN_FIFTEEN
                # 15:00:00
                if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                    return trading_day + ' ' + KTIME_FOURTEEN_FORTYFIVE
                # 21:30:00
                if cur_time >= TIME_TWENTYONE_THIRTY and last_update_time < TIME_TWENTYONE_THIRTY:
                    return trading_day + ' ' + KTIME_TWENTYONE
                # 22:00:00
                if cur_time >= TIME_TWENTYTWO and last_update_time < TIME_TWENTYTWO:
                    return trading_day + ' ' + KTIME_TWENTYONE_THIRTY
                # 22:30:00
                if cur_time >= TIME_TWENTYTWO_THIRTY and last_update_time < TIME_TWENTYTWO_THIRTY:
                    return trading_day + ' ' + KTIME_TWENTYTWO
                # 23:00:00
                if cur_time >= TIME_TWENTYTHREE and last_update_time < TIME_TWENTYTHREE:
                    return trading_day + ' ' + KTIME_TWENTYTWO_THIRTY
                # 23:30:00
                if cur_time >= TIME_TWENTYTHREE_THIRTY and last_update_time < TIME_TWENTYTHREE_THIRTY:
                    return trading_day + ' ' + KTIME_TWENTYTHREE
                # 00:00:00
                #if cur_time == TIME_ZERO:
                #    TIME_ZERO_NEW = TIME_ZERO_2
                #else:
                #    TIME_ZERO_NEW = TIME_ZERO
                # 临界时间
                #zero_dt_str = trading_day + ' ' + TIME_ZERO
                #if cur_dt_str >= zero_dt_str and end_dt_str < zero_dt_str:
                t_cur_hour = int(cur_time[0:2])
                t_end_hour = int(last_update_time[0:2])
                if (t_cur_hour <= 11 and t_cur_hour >= 0) and (t_end_hour < 24 and t_end_hour >= 23):
                    return trading_day + ' ' + KTIME_TWENTYTHREE_THIRTY
                if (t_cur_hour <= 11 and t_cur_hour >= 0) and (t_end_hour < 23 and t_end_hour >= 22):
                    return trading_day + ' ' + KTIME_TWENTYTWO_THIRTY

                # 00:30:00
                if cur_time >= TIME_ZERO_THIRTY and last_update_time < TIME_ZERO_THIRTY:
                    return trading_day + ' ' + KTIME_ZERO
                # 01:00:00
                if cur_time >= TIME_ONE and last_update_time < TIME_ONE:
                    return trading_day + ' ' + KTIME_ZERO_THIRTY
                # 01:30:00
                if cur_time >= TIME_ONE_THIRTY and last_update_time < TIME_ONE_THIRTY:
                    return trading_day + ' ' + KTIME_ONE
                # 02:00:00
                if cur_time >= TIME_TWO and last_update_time < TIME_TWO:
                    return trading_day + ' ' + KTIME_ONE_THIRTY
                # 02:30:00
                if cur_time >= TIME_TWO_THIRTY and last_update_time < TIME_TWO_THIRTY:
                    return trading_day + ' ' + KTIME_TWO

        # case1: 09:30:00开盘,15:00:00收盘
        # case2: 09:30:00开盘,15:15:00收盘
        elif self._tth.check_open_time(code_prefix, OPEN_TIME2) \
                and (self._tth.check_close_time(code_prefix, CLOSE_TIME3)
                     or self._tth.check_close_time(code_prefix, CLOSE_TIME4)):
            # 9:30:00
            if cur_time >= TIME_NINE_THIRTY and last_update_time < TIME_NINE_THIRTY:
                return trading_day + ' ' + KTIME_NINE
            # 10:00:00
            if cur_time >= TIME_TEN and last_update_time < TIME_TEN:
                return trading_day + ' ' + KTIME_NINE_THIRTY
            # 10:30:00 
            if cur_time >= TIME_TEN_THIRTY and last_update_time < TIME_TEN_THIRTY:
                return trading_day + ' ' + KTIME_TEN
            # 11:00:00 
            if cur_time >= TIME_ELEVEN and last_update_time < TIME_ELEVEN:
                return trading_day + ' ' + KTIME_TEN_THIRTY
            # 11:30:00 
            if cur_time >= TIME_ELEVEN_THIRTY and last_update_time < TIME_ELEVEN_THIRTY:
                return trading_day + ' ' + KTIME_ELEVEN
            # 13:30:00
            if cur_time >= TIME_THIRTEEN_THIRTY and last_update_time < TIME_THIRTEEN_THIRTY:
                return trading_day + ' ' + KTIME_THIRTEEN
            # 14:00:00 
            if cur_time >= TIME_FOURTEEN and last_update_time < TIME_FOURTEEN:
                return trading_day + ' ' + KTIME_THIRTEEN_THIRTY
            # 14:30:00 
            if cur_time >= TIME_FOURTEEN_THIRTY and last_update_time < TIME_FOURTEEN_THIRTY:
                return trading_day + ' ' + KTIME_FOURTEEN
            # 15:00:00 
            if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                return trading_day + ' ' + KTIME_FOURTEEN_THIRTY
            # 15:15:00
            if cur_time >= TIME_FIFTEEN_FIFTEEN and last_update_time < TIME_FIFTEEN_FIFTEEN:
                return trading_day + ' ' + KTIME_FIFTEEN
        return None

    def check_out_1h(self, code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str):
        '''
        '''
        last_date_time = end_dt_str.split(' ')
        last_update_date = last_date_time[0]
        last_update_time = last_date_time[1]
        #if trading_day > end_dt_str.split(' ')[0]:
        #    return True

        t_cur_hour = int(cur_time[0:2])
        t_end_hour = int(last_update_time[0:2])

        # 09:00:00开盘, 上午有中场休息
        if self._tth.check_open_time(code_prefix, OPEN_TIME1) and self._tth.check_morning_suspend(code_prefix):
            # case6: 02:30:00收盘, 15:00:00收盘
            is_case6 = self._tth.check_close_time(code_prefix, CLOSE_TIME7)
            is_case5 = self._tth.check_close_time(code_prefix, CLOSE_TIME6)
            is_case4 = self._tth.check_close_time(code_prefix, CLOSE_TIME5)
            is_case3 = self._tth.check_close_time(code_prefix, CLOSE_TIME3)
            if is_case6:
                # TODO 是否需要判断cache中的open_time
                # 9:30:00
                if cur_time >= TIME_NINE_THIRTY and last_update_time < TIME_NINE_THIRTY:
                    return trading_day + ' ' + KTIME_TWO
                # 10:45:00
                if cur_time >= TIME_TEN_FORTYFIVE and last_update_time <  TIME_TEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_NINE_THIRTY
                # 13:45:00
                if cur_time >= TIME_THIRTEEN_FORTYFIVE and last_update_time < TIME_THIRTEEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_TEN_FOURTYFIVE
                # 14:45:00
                if cur_time >= TIME_FOURTEEN_FORTYFIVE and last_update_time < TIME_FOURTEEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_THIRTEEN_FORTYFIVE
                # 15:00:00
                if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                    return trading_day + ' ' + KTIME_FOURTEEN_FORTYFIVE
                # 22:00:00
                if cur_time >= TIME_TWENTYTWO and last_update_time < TIME_TWENTYTWO:
                    return trading_day + ' ' + KTIME_TWENTYONE
                # 23:00:00
                if cur_time >= TIME_TWENTYTHREE and last_update_time < TIME_TWENTYTHREE:
                    return trading_day + ' ' + KTIME_TWENTYTWO
                # 00:00:00
                #if cur_time == TIME_ZERO:
                #    TIME_ZERO_NEW = TIME_ZERO_2
                #else:
                #   TIME_ZERO_NEW = TIME_ZERO
                #zero_dt_str = trading_day + ' ' + TIME_ZERO
                #if cur_dt_str >= zero_dt_str and end_dt_str < zero_dt_str:
                if (t_cur_hour <= 11 and t_cur_hour >= 0) and (t_end_hour == 23):
                    return trading_day + ' ' + KTIME_TWENTYTHREE
                # 01:00:00
                if cur_time >= TIME_ONE and last_update_time < TIME_ONE:
                    return trading_day + ' ' + KTIME_ZERO
                # 02:00:00
                if cur_time >= TIME_TWO and last_update_time < TIME_TWO:
                    return trading_day + ' ' + KTIME_ONE

            # case5: 01:00:00收盘, 15:00:00收盘
            # case4: 23:00:00收盘, 15:00:00收盘
            # case3: 15:00:00收盘
            elif is_case5 or is_case4 or is_case3:
                # 10:00:00
                if cur_time >= TIME_TEN and last_update_time < TIME_TEN:
                    return trading_day + ' ' + KTIME_NINE
                # 11:15:00
                if cur_time >= TIME_ELEVEN_FIFTEEN and last_update_time < TIME_ELEVEN_FIFTEEN:
                    return trading_day + ' ' + KTIME_TEN
                # 14:15:00
                if cur_time >= TIME_FOURTEEN_FIFTEEN and last_update_time < TIME_FOURTEEN_FIFTEEN:
                    return trading_day + ' ' + KTIME_ELEVEN_FIFTEEN
                # 15:00:00
                if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                    return trading_day + ' ' + KTIME_FOURTEEN_FIFTEEN
                # 22:00:00 
                if cur_time >= TIME_TWENTYTWO and last_update_time < TIME_TWENTYTWO:
                    return trading_day + ' ' + KTIME_TWENTYONE
                # case5
                # 23:00:00 
                if cur_time >= TIME_TWENTYTHREE and last_update_time < TIME_TWENTYTHREE:
                    return trading_day + ' ' + KTIME_TWENTYTWO
                # 00:00:00
                #if cur_time == TIME_ZERO:
                #    TIME_ZERO_NEW = TIME_ZERO_2
                #else:
                #    TIME_ZERO_NEW = TIME_ZERO
                #zero_dt_str = trading_day + ' ' + TIME_ZERO
                #if cur_dt_str >= zero_dt_str and end_dt_str < zero_dt_str:
                # case4 & case5

                # case 5
                if (t_cur_hour <= 1 and t_cur_hour >= 0) and (t_end_hour == 23):
                    return trading_day + ' ' + KTIME_TWENTYTHREE
                if (t_cur_hour <= 11 and t_cur_hour >= 8) and (t_end_hour == 22):
                    if code_prefix == 'SA':
                        return last_update_date + ' ' + KTIME_TWENTYTWO
                    return trading_day + ' ' + KTIME_TWENTYTWO
                # 01:00:00
                if cur_time >= TIME_ONE and last_update_time < TIME_ONE:
                    return trading_day + ' ' + KTIME_ZERO

        # case1: 09:30:00开盘,15:15:00收盘
        # case2: 09:30:00开盘,15:15:00收盘
        elif self._tth.check_open_time(code_prefix, OPEN_TIME2) \
                and (self._tth.check_close_time(code_prefix, CLOSE_TIME3)
                     or self._tth.check_close_time(code_prefix, CLOSE_TIME4)):
            # 10:30:00
            if cur_time >= TIME_TEN_THIRTY and last_update_time < TIME_TEN_THIRTY:
                return trading_day + ' ' + KTIME_NINE_THIRTY
            # 11:30:00 
            if cur_time >= TIME_ELEVEN_THIRTY and last_update_time < TIME_ELEVEN_THIRTY:
                return trading_day + ' ' + KTIME_TEN_THIRTY
            # 14:00:00 
            if cur_time >= TIME_FOURTEEN and last_update_time < TIME_FOURTEEN:
                return trading_day + ' ' + KTIME_THIRTEEN
            # 15:00:00 
            if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                return trading_day + ' ' + KTIME_FOURTEEN
            # 15:15:00
            if cur_time >= TIME_FIFTEEN_FIFTEEN and last_update_time < TIME_FIFTEEN_FIFTEEN:
                return trading_day + ' ' + KTIME_FIFTEEN
        return None

    def check_out_2h(self, code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str):
        '''
        '''
        last_date_time = end_dt_str.split(' ')
        last_update_date = last_date_time[0]
        last_update_time = last_date_time[1]
        #if trading_day > end_dt_str.split(' ')[0]:
        #    return True
        t_cur_hour = int(cur_time[0:2])
        t_end_hour = int(last_update_time[0:2])

        # 09:00:00开盘, 上午有中场休息
        if self._tth.check_open_time(code_prefix, OPEN_TIME1) and self._tth.check_morning_suspend(code_prefix):
            # case6: 02:30:00收盘, 15:00:00收盘
            is_case6 = self._tth.check_close_time(code_prefix, CLOSE_TIME7)
            is_case5 = self._tth.check_close_time(code_prefix, CLOSE_TIME6)
            is_case4 = self._tth.check_close_time(code_prefix, CLOSE_TIME5)
            is_case3 = self._tth.check_close_time(code_prefix, CLOSE_TIME3)
            if is_case6:
                # TODO 是否需要判断cache中的open_time
                # 9:30:00
                if cur_time >= TIME_NINE_THIRTY and last_update_time < TIME_NINE_THIRTY:
                    return trading_day + ' ' + KTIME_ONE
                # 13:45:00
                if cur_time >= TIME_THIRTEEN_FORTYFIVE and last_update_time < TIME_THIRTEEN_FORTYFIVE:
                    return trading_day + ' ' + KTIME_NINE_THIRTY
                # 15:00:00
                if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                    return trading_day + ' ' + KTIME_THIRTEEN_FORTYFIVE
                # 23:00:00
                if cur_time >= TIME_TWENTYTHREE and last_update_time < TIME_TWENTYTHREE:
                    return trading_day + ' ' + KTIME_TWENTYONE
                # 01:00:00
                if cur_time >= TIME_ONE and last_update_time < TIME_ONE:
                    return trading_day + ' ' + KTIME_TWENTYTHREE

            # case5: 01:00:00收盘, 15:00:00收盘
            # case4: 23:00:00收盘, 15:00:00收盘
            # case3: 15:00:00收盘
            elif is_case5 or is_case4 or is_case3:
                # 11:15:00
                if cur_time >= TIME_ELEVEN_FIFTEEN and last_update_time < TIME_ELEVEN_FIFTEEN:
                    return trading_day + ' ' + KTIME_NINE
                # 15:00:00 
                if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                    return trading_day + ' ' + KTIME_ELEVEN_FIFTEEN
                if cur_time >= TIME_TWENTYTHREE and last_update_time < TIME_TWENTYTHREE:
                    return trading_day + ' ' + KTIME_TWENTYONE
                # 23:00:00
                if (t_cur_hour <= 11 and t_cur_hour >= 0) and (t_end_hour < 23 and t_end_hour >= 21):
                    if code_prefix == 'SA':
                        return last_update_date + ' ' + KTIME_TWENTYONE
                    return trading_day + ' ' + KTIME_TWENTYONE
                # 01:00:00 
                if cur_time >= TIME_ONE and last_update_time < TIME_ONE:
                    return trading_day + ' ' + KTIME_TWENTYTHREE

        # case1: 09:30:00开盘,15:15:00收盘
        # case2: 09:30:00开盘,15:15:00收盘
        elif self._tth.check_open_time(code_prefix, OPEN_TIME2) \
                and (self._tth.check_close_time(code_prefix, CLOSE_TIME3)
                     or self._tth.check_close_time(code_prefix, CLOSE_TIME4)):
            # 11:30:00
            if cur_time >= TIME_ELEVEN_THIRTY and last_update_time < TIME_ELEVEN_THIRTY:
                return trading_day + ' ' + KTIME_NINE_THIRTY
            # 15:00:00 
            if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
                return trading_day + ' ' + KTIME_THIRTEEN
            # 15:15:00
            if cur_time >= TIME_FIFTEEN_FIFTEEN and last_update_time < TIME_FIFTEEN_FIFTEEN:
                return trading_day + ' ' + KTIME_FIFTEEN
        return None

    def check_out_1d(self, code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str):
        '''
        '''
        last_update_time = end_dt_str.split(' ')[1]
        # case2: 15:15:00收盘
        if self._tth.check_close_time(code_prefix, CLOSE_TIME4):
            # 15:15:00
            if cur_time >= TIME_FIFTEEN_FIFTEEN and last_update_time < TIME_FIFTEEN_FIFTEEN:
                return trading_day

        if cur_time >= TIME_FIFTEEN and last_update_time < TIME_FIFTEEN:
            return trading_day
        return None

    def gen_kline(self, *args, **kwargs):
        code, period, k_time, cur_sec, cur_dt_str, cur_dt = args[0], args[1], args[2], args[3], args[4], args[5]

        cache = self._kline_cache[code][period]
        if cache is None:
            print("error:", period, kwargs['depth'].update_time, kwargs['close_out'])
            for k, v in self._kline_cache[code].items():
                print(k, v is None)

        k_line = KLine(code, period, k_time,
                       cache.open, cache.high, cache.low, cache.close,
                       cache.volume, cache.open_interest, cache.turnover)

        # save kline
        if k_line.open > 0.0 and k_line.high > 0.0 and k_line.low > 0.0 and k_line.close > 0.0:
            if self._data_source == 'mq':
                self.producer.send(FUTURES_KLINE_TPOIC, k_line.print_line().encode('utf-8'), partition=0)
            else:
                with open('k_line_{}.csv'.format(self._data_source), 'a') as w:
                    w.write(k_line.print_line() + '\n')
        
        if kwargs['close_out']:
            # 一天收盘
            self._kline_cache[code][period] = None
        else:
            if code not in self._kline_cache or GLOBAL_CACHE_KEY not in self._kline_cache[code]:
                logger.error('[update_cache]no cache, code:{}'.format(code))
                return k_line
            # 更新GLOBAL_CACHE
            self._kline_cache[code][GLOBAL_CACHE_KEY].end_dt_str = cur_dt_str
            # 更新对应周期缓存
            self.post_update_cache(cache, cur_sec, cur_dt_str, cur_dt, depth=kwargs['depth'])
        return k_line

    def depth_tick(self, cur_depth, close_out=False, mock_end_dt_str=None):
        '''
        获取新的depth，非初始化进行正常更新
        '''
        code = cur_depth.instrument_id
        code_prefix = self._tth.get_code_prefix(code)

        if close_out:
            assert mock_end_dt_str is not None
            end_dt_str = mock_end_dt_str
        else:
            assert code in self._kline_cache
            g_cache = self._kline_cache[code][GLOBAL_CACHE_KEY]
            if g_cache is None:
                logger.error('[depth_tick]global_cache is none, code:{}, update_time:{}, update_millisec:{}'.format(
                    code,
                    cur_depth.update_time,
                    cur_depth.update_millisec))
                return
            end_dt_str = g_cache.end_dt_str

        cur_dt_str = cur_depth.action_dt_str
        end_sec, end_min, end_hour, end_day, end_month, end_year = dt_util.detail_from_str(end_dt_str)
        cur_sec, cur_min, cur_hour, cur_day, cur_month, cur_year = dt_util.detail_from_str(cur_dt_str)

        end_dt = dt_util.dt_from_str(end_dt_str)
        cur_dt = dt_util.dt_from_str(cur_dt_str)

        cur_time = cur_depth.update_time

        # 集合竞价
        if cur_depth.is_call_auction \
            or (self._tth.check_open_time(code_prefix, cur_time) and self._last_depth[code].is_call_auction):
            self.update_cache(code, M_PERIOD_KEY, cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            logger.debug('[depth_tick]auction, code:{}, update_time:{}'.format(
                code,
                cur_depth.update_time))
            return

        # 最后depth
        # 10:15, 11:30, 23:00, 01:00, 02:30
        is_suspend_times = (self._tth.check_morning_suspend(code_prefix) and cur_time == TIME_TEN_FIFTEEN) \
            or cur_time == TIME_ELEVEN_THIRTY \
            or (self._tth.check_close_time(code_prefix, CLOSE_TIME5) and cur_time == TIME_TWENTYTHREE) \
            or (self._tth.check_close_time(code_prefix, CLOSE_TIME6) and cur_time == TIME_ONE) \
            or (self._tth.check_close_time(code_prefix, CLOSE_TIME7) and cur_time == TIME_TWO_THIRTY)
        # 15:00, 15:15
        is_finish_times = (self._tth.check_close_time(code_prefix, CLOSE_TIME3) and cur_time == TIME_FIFTEEN) \
            or (self._tth.check_close_time(code_prefix, CLOSE_TIME4) and cur_time == TIME_FIFTEEN_FIFTEEN)

        if is_suspend_times or is_finish_times or close_out:
            if is_suspend_times:
                for p_key in M_PERIOD_KEY:
                    if close_out:
                        k_time = self.get_show_ktime(code_prefix, p_key,
                                                     cur_depth.trading_day, end_sec, end_min, end_hour, cur_time,
                                                     end_dt_str, cur_dt_str, end_dt, cur_dt)
                        if not k_time:
                            continue
                        if self._kline_cache[code][p_key] is None:
                            continue
                        k_line = self.gen_kline(code, p_key, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=True)
                        #self.k_lines.append(k_line)
                        # 清空缓存
                        #self._kline_cache[code][p_key] = None
                    else:
                        # 休市或收盘时间，不更新缓存end_time
                        self.update_cache(code, [p_key], cur_sec, cur_dt_str, cur_dt, depth=cur_depth, dnot_update_end_time=True)
            if is_finish_times:
                for p_key in M_PERIOD_KEY:
                    if close_out:
                        k_time = self.get_show_ktime(code_prefix, p_key,
                                                     cur_depth.trading_day, end_sec, end_min, end_hour, cur_time,
                                                     end_dt_str, cur_dt_str, end_dt, cur_dt)
                        if not k_time:
                            continue
                        if self._kline_cache[code][p_key] is None:
                            continue
                        k_line = self.gen_kline(code, p_key, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=True)
                        #self.k_lines.append(k_line)
                    else:
                        # 休市或收盘时间，不更新缓存中的end_time
                        self.update_cache(code, [p_key], cur_sec, cur_dt_str, cur_dt, depth=cur_depth, dnot_update_end_time=True)
                # 清空缓存
                #self._kline_cache[code] = {}
            return

        # 15s
        k_time = self.check_out_sec(15, cur_depth.trading_day, end_sec, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_15S] is not None and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_15S, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, M_PERIOD_KEY, cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

        # 30s 
        k_time = self.check_out_sec(30, cur_depth.trading_day, end_sec, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_30S] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_30S, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, M_PERIOD_KEY[1:], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

        # 1m
        k_time = self.check_out_min(1, cur_depth.trading_day, end_sec, end_min, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_1M] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_1M, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, M_PERIOD_KEY[2:], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

        # 3m
        k_time = self.check_out_min(3, cur_depth.trading_day, end_sec, end_min, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_3M] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_3M, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, [KEY_K_3M], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)

        # 5m
        k_time = self.check_out_min(5, cur_depth.trading_day, end_sec, end_min, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_5M] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_5M, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, M_PERIOD_KEY[4:], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

        # 15m
        k_time = self.check_out_min(15, cur_depth.trading_day, end_sec, end_min, end_dt, cur_dt)
        if k_time is not None and self._kline_cache[code][KEY_K_15M] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_15M, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, M_PERIOD_KEY[5:], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

        # 30m
        k_time = self.check_out_30m(code_prefix, cur_depth.trading_day, cur_time, end_dt_str, cur_dt_str)
        if code in self._code_auction_hour and self._code_auction_hour[code] <= TIME_NINE:
            if k_time and k_time.split(' ')[1] < TIME_NINE:
                k_time = None
        if k_time is not None and self._kline_cache[code][KEY_K_30M] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_30M, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, [KEY_K_30M], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)

        # 1h
        k_time = self.check_out_1h(code_prefix, cur_depth.trading_day, cur_time, end_dt_str, cur_dt_str)
        if code in self._code_auction_hour and self._code_auction_hour[code] <= TIME_NINE:
            if k_time and k_time.split(' ')[1] < TIME_NINE:
                k_time = None
        if k_time is not None and self._kline_cache[code][KEY_K_1H] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_1H, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, [KEY_K_1H], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)

        # 2h
        k_time = self.check_out_2h(code_prefix, cur_depth.trading_day, cur_time, end_dt_str, cur_dt_str)
        if code in self._code_auction_hour and self._code_auction_hour[code] <= TIME_NINE:
            if k_time and k_time.split(' ')[1] < TIME_NINE:
                k_time = None
        if k_time is not None and self._kline_cache[code][KEY_K_2H] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_2H, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, [KEY_K_2H], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)

        # 1d
        k_time = self.check_out_1d(code_prefix, cur_depth.trading_day, cur_time, end_dt_str, cur_dt_str)
        if k_time is not None and self._kline_cache[code][KEY_K_1D] and not self._last_depth[code].is_call_auction:
            k_line = self.gen_kline(code, KEY_K_1D, k_time, cur_sec, cur_dt_str, cur_dt, depth=cur_depth, close_out=close_out)
            #self.k_lines.append(k_line)
        else:
            self.update_cache(code, [KEY_K_1D], cur_sec, cur_dt_str, cur_dt, depth=cur_depth)
            return

    def get_show_ktime(self, code_prefix, p_key, trading_day, end_sec, end_min, end_hour, cur_time,
                       end_dt_str, cur_dt_str, end_dt, cur_dt):
        # 没有10:15的情形
        if p_key == KEY_K_15S:
            return self.check_out_sec(15, trading_day, end_sec, end_dt, cur_dt)
        elif p_key == KEY_K_30S:
            return self.check_out_sec(30, trading_day, end_sec, end_dt, cur_dt)
        elif p_key == KEY_K_1M:
            return self.check_out_min(1, trading_day, end_sec, end_min, end_dt, cur_dt)
        elif p_key == KEY_K_3M:
            return self.check_out_min(3, trading_day, end_sec, end_min, end_dt, cur_dt)
        elif p_key == KEY_K_5M:
            return self.check_out_min(5, trading_day, end_sec, end_min, end_dt, cur_dt)
        elif p_key == KEY_K_15M:
            return self.check_out_min(15, trading_day, end_sec, end_min, end_dt, cur_dt)
        elif p_key == KEY_K_30M:
            return self.check_out_30m(code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str)
        elif p_key == KEY_K_1H:
            return self.check_out_1h(code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str)
        elif p_key == KEY_K_2H:
            return self.check_out_2h(code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str)
        elif p_key == KEY_K_1D:
            return self.check_out_1d(code_prefix, trading_day, cur_time, end_dt_str, cur_dt_str)
        return None

    def gen_ktime_sec(self, sec, end_sec, end_dt):
        secs = (int(end_sec / sec) + 1) * sec - end_sec
        return dt_util.str_from_dt(end_dt + datetime.timedelta(seconds=secs))

    def gen_ktime_min(self, minute, end_sec, end_min, end_dt):
        secs = (int(end_min / minute) + 1) * minute * 60 - (end_min * 60 + end_sec)
        return dt_util.str_from_dt(end_dt + datetime.timedelta(seconds=secs))

    def gen_ktime_hour(self, hour, end_sec, end_min, end_hour, end_dt):
        secs = (int(end_hour / hour) + 1) * hour * 3600 - (end_hour * 3600 + end_min * 60 + end_sec)
        return dt_util.str_from_dt(end_dt + datetime.timedelta(seconds=secs))

    def gen_ktime_day(self, day, end_sec, end_min, end_hour, end_day, end_dt):
        #days = (int(end_day / day) + 1) * day - end_day
        #new_dt = end_dt + datetime.timedelta(days=days)
        return end_day.split(' ')[0]

    def post_update_cache(self, *args, depth=None):
        '''
        生成k线之后，用当前depth更新缓存
        '''
        cache, cur_sec, cur_dt_str, cur_dt = args[0], args[1], args[2], args[3]

        # time
        cache.open_dt_str = cur_dt_str
        cache.open_dt = cur_dt

        cache.end_sec = cur_sec
        cache.end_dt_str = cur_dt_str
        cache.end_dt = cur_dt

        # price
        if depth.volume_delta > 0.0:
            cache.open = depth.last_price
            cache.high = depth.last_price
            cache.low = depth.last_price
            cache.close = depth.last_price
            cache.volume = depth.volume_delta
            cache.open_interest = depth.open_interest_delta
            cache.turnover = depth.turnover_delta
        else:
            cache.open, cache.high, cache.low, cache.close = 0.0, 0.0, 0.0, 0.0
            cache.volume, cache.open_interest, cache.turnover = 0.0, 0.0, 0.0

        # volume
        #if depth.volume_delta > 0.0:
        #cache.volume = depth.volume_delta
        #cache.open_interest = depth.open_interest_delta
        #cache.turnover = depth.turnover_delta

    def update_cache(self, *args, depth=None, dnot_update_end_time=False):
        '''
        未生成K时正常更新缓存
        '''
        code, periods, cur_sec, cur_dt_str, cur_dt = args[0], args[1], args[2], args[3], args[4]
        if code not in self._kline_cache or GLOBAL_CACHE_KEY not in self._kline_cache[code]:
            logger.error('[update_cache]no cache, code:{}'.format(code))
            return

        if not dnot_update_end_time:
            self._kline_cache[code][GLOBAL_CACHE_KEY].end_dt_str = cur_dt_str

        for period in periods:
            if self._kline_cache[code][period] is None:
                # 休市&收盘之后的正常depth
                cache = KCache(code, cur_dt_str,
                    depth.last_price, depth.last_price, depth.last_price, depth.last_price,
                    depth.volume_delta, depth.open_interest_delta, depth.turnover_delta
                )
                self._kline_cache[code][period] = cache
            else:
                cache = self._kline_cache[code][period]
                # end time
                if not dnot_update_end_time:
                    cache.end_sec = cur_sec
                    cache.end_dt_str = cur_dt_str
                    cache.end_dt = cur_dt

                # open, high, low, close
                if depth.volume_delta > 0:
                    if cache.open == 0.0:
                        cache.open = depth.last_price
                    cache.high = max(cache.high, depth.last_price)
                    if cache.low == 0.0:
                        cache.low = depth.last_price
                    else:
                        cache.low = min(cache.low, depth.last_price)
                    cache.close = depth.last_price
                    cache.volume += depth.volume_delta
                    cache.open_interest += depth.open_interest_delta
                    cache.turnover += depth.turnover_delta

    def consume(self, cur_depth):
        assert cur_depth is not None
        code = cur_depth.instrument_id
        code_prefix = self._tth.get_code_prefix(code)

        if not self._closeout_event.isSet():
            self._closeout_event.set()

        # 集合竞价
        if self._tth.check_in(2, code, cur_depth.update_time):
            logger.debug('[consume]auction, code:{}, update_time:{}'.format(code, cur_depth.update_time))
            cur_depth.is_call_auction = True
            self._code_auction_hour[code] = cur_depth.update_time

        # 清空last_depth记录
        if code_prefix == 'SA':
            # 只有存在last_depth才需要清空
            if code in self._last_depth:
                last_hour = int(self._last_depth[code].update_time[0:2])
                t_hour = int(cur_depth.update_time[0:2])
                if (last_hour in [14, 15] and t_hour in [8, 9, 20, 21]) or cur_depth.is_call_auction:
                    logger.debug('[consume]clear last depth, code:{}, update_time:{}'.format(code, cur_depth.update_time))
                    self._last_depth.pop(code)
        else:
            if code in self._last_depth and cur_depth.trading_day != self._last_depth[code].trading_day:
                logger.debug('[consume]clear last depth, code:{}, update_time:{}'.format(code, cur_depth.update_time))
                self._last_depth.pop(code)

        # 计算delta并记录last depth
        if code in self._last_depth:
            cur_depth.volume_delta = cur_depth.volume - self._last_depth[code].volume
            cur_depth.open_interest_delta = cur_depth.open_interest - self._last_depth[code].open_interest
            cur_depth.turnover_delta = cur_depth.turnover - self._last_depth[code].turnover
        else:
            cur_depth.volume_delta = cur_depth.volume
            cur_depth.open_interest_delta = cur_depth.open_interest
            cur_depth.turnover_delta = cur_depth.turnover

        # 第一条depth
        if code not in self._kline_cache or len(self._kline_cache[code]) == 0:
            logger.debug('[consume]first depth, code:{}, update_time:{}'.format(code, cur_depth.update_time))
            self.init_cache(cur_depth)
            self._last_depth[code] = cur_depth
            return

        # 核心方法
        self.depth_tick(cur_depth)

        # 记录当前depth
        self._last_depth[code] = cur_depth

        #if len(self._k_lines) > 0:
        #    last_k = self._k_lines[-1]
        #    if last_k.open == 0.0 and last_k.high == 0.0 and last_k.low == 0.0 and last_k.close == 0.0:
        #        self._k_lines.pop()

    def init_cache(self, init_depth):
        pd_cache = {}
        code = init_depth.instrument_id

        cur_dt_str = init_depth.action_dt_str

        # global cache
        g_cache = KCache(code, cur_dt_str)
        pd_cache[GLOBAL_CACHE_KEY] = g_cache

        #start_dt = dt_util.dt_from_str(cur_dt_str)
        for item in M_PERIOD_KEY:
            cache = KCache(code, cur_dt_str)

            #if depth.volume_delta > 0.0:
            #cache.open = depth.last_price
            #cache.high = depth.last_price
            #cache.low = depth.last_price
            #cache.close = depth.last_price
            #cache.volume = depth.volume_delta
            #cache.open_interest = depth.open_interest_delta
            #cache.turnover = depth.turnover_delta
            #else:
            #    cache.open, cache.high, cache.low, cache.close = 0.0, 0.0, 0.0, 0.0
            #   cache.volume, cache.open_interest, cache.turnover = 0.0, 0.0, 0.0

            cache.open = init_depth.last_price
            cache.high = init_depth.last_price
            cache.low = init_depth.last_price
            cache.close = init_depth.last_price

            cache.volume = init_depth.volume
            cache.open_interest = init_depth.open_interest
            cache.turnover = init_depth.turnover

            pd_cache[item] = cache
        self._kline_cache[code] = pd_cache

def depth_data_iterate(data: 'str'):
    cur_msg = data.split(',')
    depth = Depth(cur_msg)
    code = depth.instrument_id
    code_prefix = tth.get_code_prefix(code)

    # 非交易时段(成交量)
    if depth.volume == 0.0:
        return
    if not tth.check_in(0, code, depth.update_time) and not tth.check_in(2, code, depth.update_time):
        return

    if code not in m_code_depth_status:
        m_code_depth_status[code] = [False, False]
    if code not in m_code_next_span:
        m_code_next_span[code] = ''

    # 重复数据过滤
    if len(m_code_next_span[code]) > 0:
        d_hour = depth.update_time[0:2]
        span = m_code_next_span[code].split(',')
        if ((d_hour < span[1] and d_hour >= span[0]) or (d_hour <= span[1] and d_hour > span[0])) \
                or ((d_hour < span[3] and d_hour >= span[2]) or (d_hour <= span[3] and d_hour > span[2])):
            m_code_depth_status[code][0] = True
            m_code_depth_status[code][1] = False
        elif (d_hour == span[1] and d_hour == span[0]) \
                or (d_hour == span[3] and d_hour == span[2]):
            if m_code_depth_status[code][1]:
                m_code_depth_status[code][0] = False
            else:
                m_code_depth_status[code][0] = True
        else:
            m_code_depth_status[code][0] = False
            m_code_depth_status[code][1] = True

        # 是否跳过
        if not m_code_depth_status[code][0]:
            return

    if depth.update_time[0:2] == '00':
        if tth.check_close_time(code_prefix, CLOSE_TIME6):
            m_code_next_span[code] = '00,00,08,11'
        elif tth.check_close_time(code_prefix, CLOSE_TIME7):
            m_code_next_span[code] = '00,00,01,02'
    elif depth.update_time[0:2] == '02':
        m_code_next_span[code] = '02,02,08,11'
    elif depth.update_time[0:2] == '11':
        m_code_next_span[code] = '11,11,13,15'
    elif depth.update_time[0:2] == '15':
        # 15点之后不过滤
        m_code_next_span[code] = ''
    elif depth.update_time[0:2] == '23':
        if tth.check_close_time(code_prefix, CLOSE_TIME5):
            m_code_next_span[code] = '23,23,08,11'
        elif tth.check_close_time(code_prefix, CLOSE_TIME6):
            m_code_next_span[code] = '23,23,00,00'
        elif tth.check_close_time(code_prefix, CLOSE_TIME7):
            m_code_next_span[code] = '23,23,00,02'

    # 计算分桶
    if code in m_ccode_id:
        b_offset = m_ccode_id[code]
    else:
        b_offset = len(m_ccode_id)
        m_ccode_id[code] = b_offset
        print("bucket:", code, b_offset)

    b_id = b_offset % NUM_HANDLER
    # 广播消息
    queues[b_id].put(depth, True, HACK_DELAY)

if __name__ == '__main__':
    # 解析参数
    args = parse_args()
    assert args.depth_source

    # k线存储位置
    if args.depth_source in MESSAGE_SOURCE:
        data_source = 'mq'
    else:
        data_source = args.depth_source.split('/')[-1].split('.')[0]

    # 并发处理
    depth_event = threading.Event()
    consumers = []
    for consumerId in range(NUM_HANDLER):
        consumer = KHandlerThread(consumerId, depth_event, data_source)
        consumers.append(consumer)
        consumer.start()
    for consumer in consumers:
        consumer.join(HACK_DELAY)

    depth_event.set()
    # 记录分桶
    m_ccode_id = {}
    # 时间处理
    tth = TranTimeHelper()

    m_code_next_span = {}
    m_code_depth_status = {}

    if args.depth_source == 'kafka':
        consumer = KafkaConsumer(FUTURES_DEPTH_TOPIC, auto_offset_reset='earliest', bootstrap_servers= ['localhost:9092'])
        for msg_data in consumer:
            assert len(msg_data) != 0
            depth_data_iterate(msg_data)
    else:
        with open(args.depth_source, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                assert len(line) != 0

                depth_data_iterate(line)
        print('File read ended!!!')
