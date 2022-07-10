import time
import sys

sys.path.append("..")
from utils import dt_util

class Depth(object):
    def __init__(self, msg, sys_time=None):
        if len(msg) >= 34:
            self.trading_day = msg[0]
            self.instrument_id = msg[1]
            self.last_price = float(msg[2])
            self.volume = float(msg[3])
            self.open_interest = float(msg[4])
            self.open_price = float(msg[5])
            self.highest_price = float(msg[6])
            self.lowest_price = float(msg[7])
            self.close_price = float(msg[8])
            self.turnover = float(msg[9])
            self.update_time = '0' + msg[10] if len(msg[10]) == 7 else msg[10]
            self.update_millisec = msg[11]
            self.bid_price1 = msg[12]
            self.bid_volume1 = msg[13]
            self.ask_price1 = msg[14]
            self.ask_volume1 = msg[15]
            self.bid_price2 = msg[16]
            self.bid_volume2 = msg[17]
            self.ask_price2 = msg[18]
            self.ask_volume2 = msg[19]
            self.bid_price3 = msg[20]
            self.bid_volume3 = msg[21]
            self.ask_price3 = msg[22]
            self.ask_volume3 = msg[23]
            self.bid_price4 = msg[24]
            self.bid_volume4 = msg[25]
            self.ask_price4 = msg[26]
            self.ask_volume4 = msg[27]
            self.bid_price5 = msg[28]
            self.bid_volume5 = msg[29]
            self.ask_price5 = msg[30]
            self.ask_volume5 = msg[31]
            self.average_price = msg[32]
            self.action_day = msg[33]
            self.is_call_auction = False
            self.volume_delta = 0.0
            self.open_interest_delta = 0.0
            self.turnover_delta = 0.0
            self.trading_dt_str = self.trading_day + ' ' + self.update_time

            self.action_dt_str = self.action_day + ' ' + self.update_time
            self.action_dt = dt_util.dt_from_str(self.action_dt_str)
        else:
            self.trading_day = msg[0]
            self.action_day = msg[0]
            self.update_time = '0' + msg[1] if len(msg[1]) == 7 else msg[1]
            self.instrument_id = msg[2]

            self.action_dt_str = self.action_day + ' ' + self.update_time
            self.action_dt = dt_util.dt_from_str(self.action_dt_str)

            self.last_price = 0.0
            self.volume = 0.0
            self.open_interest = 0.0
            self.open_price = 0.0
            self.highest_price = 0.0
            self.lowest_price = 0.0
            self.close_price = 0.0
            self.turnover = 0.0

            self.is_call_auction = False
            self.volume_delta = 0.0
            self.open_interest_delta = 0.0
            self.turnover_delta = 0.0
        self.sys_time = sys_time

    def refresh_update_time(self, new_update_time):
        self.action_dt_str = self.action_day + ' ' + new_update_time
        self.action_dt = dt_util.dt_from_str(self.action_dt_str)
        self.trading_dt_str = self.trading_day + ' ' + new_update_time

    def __str__(self):
        return "{},{},{},{},{},{},{},{},{}".format(
            self.trading_day,
            self.instrument_id,
            self.update_time,
            self.update_millisec,
            self.last_price,
            self.volume,
            self.open_interest,
            self.turnover,
            self.action_day)

