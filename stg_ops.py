import sys
import time

import util

def diff(x):
    _diff_d = []
    for i in range(len(x)):
        _diff_d[i] = util.ema(x[:i+1], 12) - util.ema(x[:i+1], 26)
    return _diff_d

def dea(x):
    _dea_d = []
    for i in range(len(x)):
        _dea_d[i] = util.ema(DIFF[:i+1], 9)
    return _dea_d

def macd(x):
    _diff_x = diff(x)
    _dea_x = dea(x)

    _macd_col = []
    for i in range(len(x)):
        _macd_col[i] = 2*(_diff_x[i] - _dea_x[i])

