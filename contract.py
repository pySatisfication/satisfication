import os
import json
import sys
import numpy as np
from easydict import EasyDict as edict

import utils.util as op_util
import utils.redis_ts_util as rtu
import utils.file_util as fu

_RESERVE_DAYS = 300
_EMA12_DEFAULT_N = 12
_EMA26_DEFAULT_N = 26
_DEA_DEFAULT_N = 9
#_DMI_DEFAULT_N = 14
_TR_DEFAULT_N = 14
_TR_DEFAULT_M = 1
_DMP_DEFAULT_N = 14
_DMP_DEFAULT_M = 1
_DMM_DEFAULT_N = 14
_DMM_DEFAULT_M = 1
_ADX_DEFAULT_N = 14
_ADX_DEFAULT_M = 1
_GUPPY_DEFAULT_N = 2

class EventPoint(object):
    def __init__(self, event, start, end, close_idx):
        self._event = event
        self._start_time = start
        self._end_time = end
        self._close_idx = close_idx
        
    @property
    def close_idx(self):
        return self._close_idx

    @property
    def end_time(self):
        return self._end_time

    @property
    def event_name(self):
        return self._event

    def print(self):
        return 'type:{},start:{},end:{},close_idx:{}'.format(
            self._type, self._start_time, self._end_time, self._close_idx)

class Contract(object):
    def __init__(self, c_code):
        self._c_code = c_code

        # last_time
        self._time = []

        # prices and transaction 
        self._open = []
        self._high = []
        self._low = []
        self._close = []
        self._volumes = []
        self._holds = []
        self._amounts = []
        self._avg_prices = []

        # ma
        #self._ma = []
        #self._ema = []
        self._ema12 = []
        self._ema26 = []
        #self._sma = []

        # std
        #self._std = []

        # dev
        self._diff = []
        self._dea = []
        self._macd = []

        self._inters = []   # 金叉死叉点
        self._t_cross = []  # DIFF&DEA是否交叉
        self._pars = []     # 分型点

        # boll 
        self._boll_st = []
        self._ub_st = []
        self._lb_st = []
        self._trend_to_rise = []
        self._trend_to_fall = []
        self._gravity_line = []
        self._of_f_ub_st = []
        self._of_f_lb_st = []

        # dmi
        self._tr = []
        self._hd = []
        self._ld = []
        self._dmp = []
        self._dmm = []
        self._pdi = []
        self._mdi = []
        self._adx = []

        # guppy mean line
        self._boll_st_s1 = []
        self._boll_st_s2 = []
        self._boll_st_s3 = []
        self._boll_st_s4 = []
        self._boll_st_s5 = []

        # parting
        self._hh = []
        self._ll = []

        self._stg_f_boll = []
        self._stg_f_par = []
        self._stg_f_dmi = []
        self._stg_macd_dev = []
        self._stg_sep_dev = []
        self._stg_in_dev = []
        self._stg_in_sep_dev = []
        self._stg_f_guppy = []

    @property
    def inters(self):
        return self._inters

    @property
    def close(self):
        return self._close

    @property
    def ma(self):
        return self._ma

    @property
    def ema12(self):
        return self._ema12

    @property
    def ema26(self):
        return self._ema26

    @property
    def sma(self):
        return self._sma

    @property
    def t_to_rise(self):
        return self._trend_to_rise

    @property
    def t_to_fall(self):
        return self._trend_to_fall

    @property
    def gravity_line(self):
        return self._gravity_line

    @property
    def of_f_ub_st(self):
        return self._of_f_ub_st

    @property
    def of_f_lb_st(self):
        return self._of_f_lb_st

    @property
    def boll_st(self):
        return self._boll_st

    def check_data_valid(self):
        for k1, v1 in self.__dict__.items():
            if not isinstance(v, list):
                continue
            for k2, v2 in self.__dict__.items():
                if not isinstance(v, list) or k1 == k2:
                    continue
                if len(v1) != len(v2):
                    raise ValueError('inconsistent data length')

    # 存储Redis
    def clct_all_var(self, exclude_stg=True):
        m_var = {}
        for k, v in self.__dict__.items():
            if not isinstance(v, list) or len(v) == 0:
                continue
            if k in ['_inters', '_cur_t_data', '_pars']:
                continue
            if exclude_stg and k.startswith('_stg'):
                continue
            m_var[k] = v[-1]
        return json.dumps(m_var)

    # 验证
    def clct_for_validate(self):
        m_var = {}
        for k, v in self.__dict__.items():
            if not isinstance(v, list) or len(v) == 0:
                continue
            if k in ['_inters', '_cur_t_data', '_pars', 
                     '_tr', '_hd', '_ld', '_dmp', '_dmm', '_pdi', '_mdi', '_adx', 
                     '_hh', '_ll', 
                     '_ema12', '_ema26']:
                continue
            if k == '_stg_f_dmi':
                m_var[k] = v[-1][-1:]
            else:
                m_var[k] = v[-1]
        return json.dumps(m_var)

    def get_last_par_points(self, inter_time, event_name=None):
        '''
        traverse to get lastest parting points
        '''
        last_pars = []
        for idx in range(0, len(self._pars)):
            if self._pars[-idx].end_time > inter_time and self._pars[-idx].event_name == event_name:
                last_pars.insert(0, self._pars[-idx])
        return last_pars

class Option(Contract):
    def __init__(self, code):
        assert code is not None
        super(Option, self).__init__(c_code=code)
        self._type = 'option'
        self._cur_t_data = None

    @property
    def t_data(self):
        return self._cur_t_data

    @t_data.setter
    def t_data(self, value):
        if isinstance(value, dict):
            self._cur_t_data = value
        else:
            raise ValueError('value error')

    @property
    def c_code(self):
        return self._c_code

    def update_ct_data(self):
        assert self.t_data.open is not None and isinstance(self.t_data.open, float)
        assert self.t_data.high is not None and isinstance(self.t_data.high, float)
        assert self.t_data.low is not None and isinstance(self.t_data.low, float)
        assert self.t_data.close is not None and isinstance(self.t_data.close, float)

        self._open.append(self.t_data.open)
        self._open = self._open[-_RESERVE_DAYS:]

        self._high.append(self.t_data.high)
        self._high = self._high[-_RESERVE_DAYS:]

        self._low.append(self.t_data.low)
        self._low = self._low[-_RESERVE_DAYS:]

        self._close.append(self.t_data.close)
        self._close = self._close[-_RESERVE_DAYS:]

        self._volumes.append(self.t_data.volumes)
        self._volumes = self._volumes[-_RESERVE_DAYS:]

        self._holds.append(self.t_data.holds)
        self._holds = self._holds[-_RESERVE_DAYS:]

        self._amounts.append(self.t_data.amounts)
        self._amounts = self._amounts[-_RESERVE_DAYS:]

        self._avg_prices.append(self.t_data.avg_prices)
        self._avg_prices = self._avg_prices[-_RESERVE_DAYS:]

    def update_ema(self):
        """
        calculate ema in sevaral days
        """
        assert len(self._close) > 0, \
            'close sequence must be non-empty'

        # ema in 12 days
        cur_val = op_util.ema_cc(self._ema12, self._close[-1], _EMA12_DEFAULT_N)
        self._ema12.append(round(cur_val, 2))
        self._ema12 = self._ema12[-_RESERVE_DAYS:]

        # 26 days
        cur_val = op_util.ema_cc(self._ema26, self._close[-1], _EMA26_DEFAULT_N)
        self._ema26.append(round(cur_val, 2))
        self._ema26 = self._ema26[-_RESERVE_DAYS:]

    def update_macd(self):
        assert len(self._ema12) > 0 and len(self._ema26) > 0
        # diff
        self._diff.append(round(self._ema12[-1] - self._ema26[-1], 2))
        self._diff = self._diff[-_RESERVE_DAYS:]

        # dea
        cur_val = round(op_util.ema_cc(self._dea, self._diff[-1], _DEA_DEFAULT_N), 2)
        self._dea.append(cur_val)
        self._dea = self._dea[-_RESERVE_DAYS:]

        # macd
        cur_val = round(2*(self._diff[-1] - self._dea[-1]), 2)
        self._macd.append(cur_val)
        self._macd = self._macd[-_RESERVE_DAYS:]

    # boll
    def update_boll(self, N=20, M=2):
        """
        calculate boll in 3 types
        """
        if len(self._close) < N:
            N = len(self._close)

        # boll_st
        cur_boll_st = round(op_util.ma(self._close, N), 6)
        self._boll_st.append(cur_boll_st)
        self._boll_st = self._boll_st[-_RESERVE_DAYS:]

        # ub_st
        cur_ub_st = round(cur_boll_st + M * op_util.std(self._close, N), 6)
        self._ub_st.append(cur_ub_st)
        self._ub_st = self._ub_st[-_RESERVE_DAYS:]

        # lb_st
        cur_lb_st = round(cur_boll_st - M * op_util.std(self._close, N), 6)
        self._lb_st.append(cur_lb_st)
        self._lb_st = self._lb_st[-_RESERVE_DAYS:]

        # gravity_line
        cur_gl = round((self._high[-1] + self._low[-1] + self._open[-1] + 3 * self._close[-1]) / 6.0, 6)
        self._gravity_line.append(cur_gl)

        if len(self._boll_st) == 1:
            self._trend_to_rise.append(0)
            self._trend_to_fall.append(0)
            self._of_f_ub_st.append(0)
            self._of_f_lb_st.append(0)
            return

        # BOLL_ST>=REF(BOLL_ST,1) AND UB_ST>REF(UB_ST,1)
        if self._boll_st[-1] > op_util.ref(self._boll_st, 1) \
            and self._ub_st[-1] > op_util.ref(self._ub_st, 1):
            self._trend_to_rise.append(1)
        else:
            self._trend_to_rise.append(0)

        if self._boll_st[-1] <= op_util.ref(self._boll_st, 1) \
            and self._lb_st[-1] > op_util.ref(self._lb_st, 1):
            self._trend_to_fall.append(1)
        else:
            self._trend_to_fall.append(0)

        self._of_f_ub_st.append(1 if cur_gl > self._ub_st[-1] else 0)
        self._of_f_lb_st.append(1 if cur_gl < self._ub_st[-1] else 0)

    def update_guppyline(self, N = 2):
        self._boll_st_s1 = self._boll_st

        if len(self._boll_st_s1) < N:
            N = len(self._boll_st_s1)

        self._boll_st_s2.append(round(op_util.ma(self._boll_st_s1, N), 6))
        self._boll_st_s2 = self._boll_st_s2[-_RESERVE_DAYS:]

        self._boll_st_s3.append(round(op_util.ma(self._boll_st_s2, N), 6))
        self._boll_st_s3 = self._boll_st_s3[-_RESERVE_DAYS:]

        self._boll_st_s4.append(round(op_util.ma(self._boll_st_s3, N), 6))
        self._boll_st_s4 = self._boll_st_s4[-_RESERVE_DAYS:]

        self._boll_st_s5.append(round(op_util.ma(self._boll_st_s4, N), 6))
        self._boll_st_s5 = self._boll_st_s5[-_RESERVE_DAYS:]

    def update_parting(self):
        if len(self._close) <= 2:
            self._hh.append(self._high[-1])
            self._ll.append(self._low[-1])
            return

        h_1, h_2, h_3 = self._hh[-2], self._hh[-1], self._high[-1]
        l_1, l_2, l_3 = self._ll[-2], self._ll[-1], self._low[-1]
        if (h_2 < h_3 or l_2 > l_3) and (h_2 > h_3 or l_2 < l_3):
           # case 0
           self._hh.append(h_3)
           self._ll.append(l_3)
        elif (h_2 >= h_3 and l_2 <= l_3) or (h_2 <= h_3 and l_2 >= l_3):
            if (h_2 > h_1 and l_2 >= l_1) or (h_2 >= h_1 and l_2 > l_1):
                # case 1, upward
                self._hh.append(max(h_2, h_3))
                self._ll.append(max(l_2, l_3))
            elif (h_2 < h_1 and l_2 <= l_1) or (h_2 <= h_1 and l_2 < l_1):
                # case 2, downward
                self._hh.append(min(h_2, h_3))
                self._ll.append(min(l_2, l_3))
            else:
                # special judgement
                n = 3
                t_hh3 = h_3
                t_ll3 = l_3
                while n <= len(self._hh):
                    if (h_2 > self._hh[-n] and l_2 >= self._ll[-n]) or \
                       (h_2 >= self._hh[-n] and l_2 > self._ll[-n]):
                        t_hh3 = max(h_2, h_3)
                        t_ll3 = max(l_2, l_3)
                        break
                    elif (h_2 < self._hh[-n] and l_2 <= self._ll[-n]) or \
                         (h_2 <= self._hh[-n] and l_2 < self._ll[-n]):
                        t_hh3 = min(h_2, h_3)
                        t_ll3 = min(l_2, l_3)
                        break
                    n += 1
                self._hh.append(t_hh3)
                self._ll.append(t_ll3)

    def update_dmi(self):
        """
        first item in tr is max(hl, hr, lr), the following elements 
        are calulated by SMA(X,N,M)
        """
        if len(self._close) < 2 or len(self._high) < 2 or len(self._low) < 2:
            self._tr.append(0.0)
            self._hd.append(0.0)
            self._ld.append(0.0)
            self._dmp.append(0.0)
            self._dmm.append(0.0)
            self._pdi.append(0.0)
            self._mdi.append(0.0)
            self._adx.append(0.0)
            return
        
        hl = self._high[-1] - self._low[-1]
        hr = abs(self._high[-1] - op_util.ref(self._close, 1))
        lr = abs(self._low[-1] - op_util.ref(self._close, 1))
        cur_tr = max(hl, hr, lr)

        if len(self._tr) == 0:
            self._tr.append(cur_tr)
        else:
            cur_val = op_util.sma_cc(self._tr, cur_tr, _TR_DEFAULT_N, _TR_DEFAULT_M)
            self._tr.append(round(cur_val, 2))
        self._tr = self._tr[-_RESERVE_DAYS:]

        # hd&ld
        self._hd.append(self._high[-1] - op_util.ref(self._high, 1))
        self._ld.append(op_util.ref(self._low, 1) - self._low[-1])

        # dmp
        if self._hd[-1] > 0 and self._hd[-1] > self._ld[-1]:
            cur_dmp = self._hd[-1]
        else:
            cur_dmp = 0
        cur_val = op_util.sma_cc(self._dmp, cur_dmp, _DMP_DEFAULT_N, _DMP_DEFAULT_M)
        self._dmp.append(round(cur_val, 2))

        # dmm
        if self._ld[-1] > 0 and self._ld[-1] > self._hd[-1]:
            cur_dmm = self._ld[-1]
        else:
            cur_dmm = 0
        cur_val = op_util.sma_cc(self._dmm, cur_dmm, _DMM_DEFAULT_N, _DMM_DEFAULT_M)
        self._dmm.append(round(cur_val, 2))

        # pdi
        if self._tr[-1] == 0.0:
            d_tr = 1
        else:
            d_tr = self._tr[-1]
        self._pdi.append(self._dmp[-1]*100/d_tr)

        # mdi
        self._mdi.append(self._dmm[-1]*100/d_tr)

        # adx
        if self._pdi[-1] + self._mdi[-1] == 0:
            d_mp = 1
        else:
            d_mp = self._pdi[-1] + self._mdi[-1]
        cur_adx = op_util.sma_cc(self._adx, 
                                 (self._pdi[-1]-self._mdi[-1])/d_mp,
                                 _ADX_DEFAULT_N, 
                                 _ADX_DEFAULT_M)
        self._adx.append(round(cur_adx, 2))

    def check_boll(self):
        f_boll = ['0']*4
        f_boll[0] = str(self._trend_to_rise[-1])
        f_boll[1] = str(self._of_f_ub_st[-1])
        f_boll[2] = str(self._trend_to_fall[-1])
        f_boll[3] = str(self._of_f_lb_st[-1])
        return f_boll

    def check_parting(self):
        '''
        前一时刻出现的顶/底值，记录在当前时刻，因为只有到当前
        '''
        f_par = ['0']
        if len(self._close) <= 2:
            return f_par

        h_1, h_2, h_3 = self._hh[-3], self._hh[-2], self._hh[-1]
        l_1, l_2, l_3 = self._ll[-3], self._ll[-2], self._ll[-1]
        if h_2 > h_1 and h_2 > h_3 and l_2 > l_1 and l_2 > l_3:
            # 顶分型
            self._pars.append(EventPoint('TP', self._time[-2], self._time[-1], len(self._close) - 1))
            f_par[0] = '1'
        elif h_2 < h_1 and h_2 < h_3 and l_2 < l_1 and l_2 < l_3:
            # 底分型
            self._pars.append(EventPoint('BP', self._time[-2], self._time[-1], len(self._close) - 1))
            f_par[0] = '-1'

        return f_par

    def check_guppy(self):
        f_guppy = ['0'] * 2
        if self._boll_st_s1[-1] < self._boll_st_s5[-1]:
            f_guppy[0] = '-1'
        elif self._boll_st_s1[-1] >= self._boll_st_s5[-1]:
            f_guppy[0] = '1'

        if len(self._boll_st_s1) < 2:
            return f_guppy

        s1_2, s5_2 = self._boll_st_s1[-2], self._boll_st_s5[-2]
        s1_1, s5_1 = self._boll_st_s1[-1], self._boll_st_s5[-1]
        if s1_2 > s5_2 and s1_1 < s5_1:
            f_guppy[1] = '-1';
        elif s1_2 < s5_2 and s1_1 > s5_1:
            f_guppy[1] = '1';

        return f_guppy

    def check_dmi(self):
        f_dmi = ['0']*7
        if len(self._close) < 2:
            return f_dmi

        if self._adx[-1] < -30.0:
            f_dmi[0] = '1'
        elif self._adx[-1] < -16.0 and self._adx[-1] > -30.0:
            f_dmi[1] = '1'
        elif self._adx[-1] < 0 and self._adx[-1] >= -16.0:
            f_dmi[2] = '1'
        elif self._adx[-1] <=16 and self._adx[-1] > 0.0:
            f_dmi[3] = '1'
        elif self._adx[-1] <= 30 and self._adx[-1] > 16.0:
            f_dmi[4] = '1'
        elif self._adx[-1] > 30.0:
            f_dmi[5] = '1'

        # extreme point check
        if op_util.ref(self._adx, 1) > 60.0 and self._adx[-1] < 60.0:
            f_dmi[6] = '-1'
        elif op_util.ref(self._adx, 1) < -60.0 and self._adx[-1] > -60.0:
            f_dmi[6] = '1'
        else:
            f_dmi[6] = '0'

        return f_dmi

    def make_judge_jincha(self, i_a, i_b, i_c, i_d):
        f_dev = ['0']*4
        # [s, e)
        c_max1 = max(self._close[i_a.close_idx:i_b.close_idx])
        c_max2 = max(self._close[i_c.close_idx:i_d.close_idx])
        if c_max1 <= c_max2:
            f_dev[0] = '1'

        diff_max1 = max(self._diff[i_a.close_idx:i_b.close_idx])
        diff_max2 = max(self._diff[i_c.close_idx:i_d.close_idx])
        if diff_max1 > diff_max2:
            f_dev[1] = '1'

        macd_max1 = max(self._macd[i_a.close_idx:i_b.close_idx])
        macd_max2 = max(self._macd[i_c.close_idx:i_d.close_idx])
        if macd_max1 > macd_max2:
            f_dev[2] = '1'

        macd_area1 = sum([abs(m) for m in self._macd[i_a.close_idx:i_b.close_idx]])
        macd_area2 = sum([abs(m) for m in self._macd[i_c.close_idx:i_d.close_idx]])
        if macd_area1 > macd_area2:
            f_dev[3] = '1'
        return f_dev

    def make_judge_sicha(self, i_a, i_b, i_c, i_d):
        f_dev = ['0']*4
        # [s, e)
        c_max1 = max(self._close[i_a.close_idx:i_b.close_idx])
        c_max2 = max(self._close[i_c.close_idx:i_d.close_idx])
        if c_max1 >= c_max2:
            f_dev[0] = '1'

        diff_max1 = max(self._diff[i_a.close_idx:i_b.close_idx])
        diff_max2 = max(self._diff[i_c.close_idx:i_d.close_idx])
        if diff_max1 < diff_max2:
            f_dev[1] = '1'

        macd_max1 = max(self._macd[i_a.close_idx:i_b.close_idx])
        macd_max2 = max(self._macd[i_c.close_idx:i_d.close_idx])
        if macd_max1 < macd_max2:
            f_dev[2] = '1'

        macd_area1 = sum([abs(m) for m in self._macd[i_a.close_idx:i_b.close_idx]])
        macd_area2 = sum([abs(m) for m in self._macd[i_c.close_idx:i_d.close_idx]])
        if macd_area1 < macd_area2:
            f_dev[3] = '1'
        return f_dev

    def in_bottom_dev(self, l1_from, l1_to, l2_from, l2_to):
        f_in_dev = ['0']*2
        L1 = min(self._low[l1_from:l1_to])
        L2 = min(self._low[l2_from:l2_to])

        D1 = self._diff[l1_from:l1_to][np.array(self._diff[l1_from:l1_to]).argmin()]
        D2 = self._diff[l2_from:l2_to][np.array(self._diff[l2_from:l2_to]).argmin()]
        M1 = self._macd[l1_from:l1_to][np.array(self._macd[l1_from:l1_to]).argmin()]
        M2 = self._macd[l2_from:l2_to][np.array(self._macd[l2_from:l2_to]).argmin()]
        if (L2 <= L1 and D2 > D1) or (L2 < L1 and D2 >= D1):
            f_in_dev[0] = '1'   # 快线内部底背驰
        if (L2 <= L1 and M2 > M1) or (L2 < L1 and M2 >= M1):
            f_in_dev[1] = '1'   # MACD柱内部底背驰
        return f_in_dev

    def in_top_dev(self, h1_from, h1_to, h2_from, h2_to):
        f_in_dev = ['0']*2
        H1 = max(self._high[h1_from:h1_to])
        H2 = max(self._high[h2_from:h2_to])

        D1 = self._diff[h1_from:h1_to][np.array(self._diff[h1_from:h1_to]).argmax()]
        D2 = self._diff[h2_from:h2_to][np.array(self._diff[h2_from:h2_to]).argmax()]
        M1 = self._macd[h1_from:h1_to][np.array(self._macd[h1_from:h1_to]).argmax()]
        M2 = self._macd[h2_from:h2_to][np.array(self._macd[h2_from:h2_to]).argmax()]
        if (H2 >= H1 and D2 < D1) or (H2 > H1 and D2 <= D1):
            f_in_dev[0] = '1'   # 快线内部顶背驰
        if (H2 >= H1 and M2 < M1) or (H2 > H1 and M2 <= M1):
            f_in_dev[1] = '1'   # MACD柱内部顶背驰
        return f_in_dev

    def check_dev(self):
        """
        return:
            an array with length of 8,
            index 0~3       # bottom divergence
            index 4~7       # top divergence
        """
        # 普通背离
        f_macd_dev = ['0']*8
        # 普通隔山背离
        f_sep_dev = ['0']*8
        # 内部背离
        f_in_dev = ['0']*4
        # 内部隔山背离
        f_in_sep_dev = ['0']*4

        if len(self._diff) == 1:
            self._t_cross.append(0)
            return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

        if self._diff[-2] < self._dea[-2] and self._diff[-1] > self._dea[-1]:     # jincha
            # 1. record
            self._inters.append(EventPoint('K', self._time[-2], self._time[-1], len(self._close) - 1))
            self._t_cross.append(1)

            # 2. 普通底背离
            if len(self._inters) < 4 or not (self._inters[-2].event_name == 'D' and \
                self._inters[-3].event_name == 'K' and self._inters[-4].event_name == 'D'):
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_macd_dev[0:4] = self.make_judge_jincha(self._inters[-2], self._inters[-1], 
                                                self._inters[-4], self._inters[-3])

            # 3. 隔山底背离
            if len(self._inters) < 6 or not (self._inters[-5].event_name == 'K' and self._inters[-6].event_name == 'D'):
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_sep_dev[0:4] = self.make_judge_jincha(self._inters[-2], self._inters[-1], 
                                               self._inters[-6], self._inters[-5])
        elif self._diff[-2] > self._dea[-2] and self._diff[-1] < self._dea[-1]:   # sicha
            # 1. record
            self._inters.append(EventPoint('D', self._time[-2], self._time[-1], len(self._close) - 1))
            self._t_cross.append(-1)

            # 2. 普通顶背离
            if len(self._inters) < 4 or not (self._inters[-2].event_name == 'K' and \
                self._inters[-3].event_name == 'D' and self._inters[-4].event_name == 'K'):
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_macd_dev[4:8] = self.make_judge_sicha(self._inters[-2], self._inters[-1], 
                                               self._inters[-4], self._inters[-3])

            # 3. 隔山顶背离
            if len(self._inters) < 6 or not (self._inters[-5].event_name == 'D' and self._inters[-6].event_name == 'K'):
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_sep_dev[4:8] = self.make_judge_sicha(self._inters[-2], self._inters[-1], 
                                              self._inters[-6], self._inters[-5])
        else:
            self._t_cross.append(0)

        # 内部背离
        if len(self._inters) > 0 and self._inters[-1].event_name == 'D':     # sc
            par_points = self.get_last_par_points(self._inters[-1].end_time, 'BP')

            # 当前非分型点
            if len(par_points) > 0 and par_points[-1].end_time != self.t_data.time:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            # 内部普通底背离
            if len(par_points) == 2:
                l1_from, l1_to = self._inters[-1].close_idx, par_points[-2].close_idx
                l2_from, l2_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) > 2:
                l1_from, l1_to = par_points[-3].close_idx, par_points[-2].close_idx
                l2_from, l2_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) < 2:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_in_dev[0:2] = self.in_bottom_dev(l1_from, l1_to, l2_from, l2_to)

            # 内部隔山底背离
            if len(par_points) == 3:
                l1_in_from, l1_in_to = self._inters[-1].close_idx, par_points[-3].close_idx
                l2_in_from, l2_in_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) > 3:
                l1_in_from, l1_in_to = par_points[-4].close_idx, par_points[-3].close_idx
                l2_in_from, l2_in_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) < 3:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_in_sep_dev[0:2] = self.in_bottom_dev(l1_in_from, l1_in_to, l2_in_from, l2_in_to)
        elif len(self._inters) > 0 and self._inters[-1].event_name == 'K':   # jc
            par_points = self.get_last_par_points(self._inters[-1].end_time, 'TP')

            # 当前非分型点
            if len(par_points) > 0 and par_points[-1].end_time != self.t_data.time:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            # 内部普通顶背离
            if len(par_points) == 2:
                h1_from, h1_to = self._inters[-1].close_idx, par_points[-2].close_idx
                h2_from, h2_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) > 2:
                h1_from, h1_to = par_points[-3].close_idx, par_points[-2].close_idx
                h2_from, h2_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) < 2:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_in_dev[2:4] = self.in_top_dev(h1_from, h1_to, h2_from, h2_to)

            # 内部隔山顶背离
            if len(par_points) == 3:
                h1_in_from, h1_in_to = self._inters[-1].close_idx, par_points[-3].close_idx
                h2_in_from, h2_in_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) > 3:
                h1_in_from, h1_in_to = par_points[-4].close_idx, par_points[-3].close_idx
                h2_in_from, h2_in_to = par_points[-2].close_idx, par_points[-1].close_idx
            elif len(par_points) < 3:
                return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

            f_in_sep_dev[2:4] = self.in_top_dev(h1_in_from, h1_in_to, h2_in_from, h2_in_to)

        return f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev

    def iterate(self, data):
        assert isinstance(data, dict)

        self.t_data = data
        if len(self._time) > 0:
            assert data.time > self._time[-1], \
                'time of data must be greater than lastest updated time:{}'.format(
                self._time[-1])
        self._time.append(data.time)
        """
        step 1. update indicator
        """
        self.update_ct_data()

        self.update_ema()
        self.update_boll()
        self.update_guppyline()
        self.update_parting()
        self.update_macd()
        self.update_dmi()

        # debug ema
        if data.time == '202110181459':
            print('final close:', self._close)
            print('final ema12:', self._ema12)

        return self.clct_all_var()

    def make_judge(self):
        # boll
        f_boll = self.check_boll()
        self._stg_f_boll.append(''.join(f_boll))

        # parting
        f_par = self.check_parting()
        self._stg_f_par.append(''.join(f_par))

        # dmi
        f_dmi = self.check_dmi()
        self._stg_f_dmi.append(''.join(f_dmi))
        
        # dev
        f_macd_dev, f_sep_dev, f_in_dev, f_in_sep_dev = self.check_dev()
        self._stg_macd_dev.append(''.join(f_macd_dev))
        self._stg_sep_dev.append(''.join(f_sep_dev))
        self._stg_in_dev.append(''.join(f_in_dev))
        self._stg_in_sep_dev.append(''.join(f_in_sep_dev))

        # guppy
        f_guppy = self.check_guppy()
        self._stg_f_guppy.append(''.join(f_guppy))

if __name__ == '__main__':

    # read input file
    if len(sys.argv[1]) < 2:
        print('Usage: python {} path'.format(sys.argv[0]))
        exit(0)

    out_path = 'data/validate'
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    files = fu.get_files(sys.argv[1], '_6_10')
    for file_path in files:
        cc = Option('null')
        t_data = []
        with open(file_path, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                t_data.append(line)

        res = []
        row_head = []
        line_idx = 0
        for line in t_data:
            items = line.split(',')
            d_data = edict(m_code = int(items[0]), 
                           c_code = items[1],
                           time = items[2],
                           open = float(items[3]),
                           high = float(items[4]),
                           low = float(items[5]),
                           close = float(items[6]), volumes = float(items[7]),
                           holds = float(items[8]),
                           amounts = float(items[9]),
                           avg_prices = float(items[10])
                        )
            cc.iterate(d_data)
            cc.make_judge()

            # base index
            res_str = cc.clct_for_validate()
            j_res = json.loads(res_str)
            val = []
            for k, v in j_res.items():
                #print(k + '\t' + str(v))
                if line_idx == 0:
                    if k == '_volumes':
                        row_head.append('_volumes(成交量)')
                    elif k == '_holds':
                        row_head.append('_holds(持仓量)')
                    elif k == '_amounts':
                        row_head.append('_amounts(成交金额)')
                    elif k == '_avg_prices':
                        row_head.append('_avg_prices(平均成交价格)')
                    elif k == '_trend_to_rise':
                        row_head.append('_trend_to_rise(上涨趋势)')
                    elif k == '_trend_to_fall':
                        row_head.append('_trend_to_fall(下跌趋势)')
                    elif k == '_gravity_line':
                        row_head.append('_gravity_line(重心线)')
                    elif k == '_of_f_ub_st':
                        row_head.append('_of_f_ub_st(溢出上轨)')
                    elif k == '_of_f_lb_st':
                        row_head.append('_of_f_lb_st(溢出下轨)')
                    elif k == '_stg_f_par':
                        row_head.append('_stg_f_par(分型)')
                    elif k == '_stg_macd_dev':
                        row_head.append('_stg_macd_dev(普通背离)')
                    elif k == '_stg_sep_dev':
                        row_head.append('_stg_sep_dev(隔山背离)')
                    elif k == '_stg_f_guppy':
                        row_head.append('_stg_f_guppy(顾比策略)')
                    elif k == '_stg_in_dev':
                        row_head.append('_stg_in_dev(内部背离)')
                    elif k == '_stg_in_sep_dev':
                        row_head.append('_stg_in_sep_dev(内部隔山背离)')
                    elif k == '_stg_f_dmi':
                        row_head.append('_stg_f_adx_ex')
                    else:
                        row_head.append(k)
                val.append(v)
            if line_idx == 0:
                res.append(row_head)
            res.append(val)
            line_idx += 1

        res_arr = np.array(res)

        file_name = file_path.split('/')[-1]
        print(file_path, file_name)

        tips = '指标说明：\n \
        1、顾比策略: \n \
            位置1: boll_st_s1对比boll_st_s5（-1: 小于, 1:大于等于）\n \
            位置2: boll_st_s1是否穿过boll_st_s5（0: 未交叉, 1: s1上穿s5, -1: s1下穿s5）\n \
        2、背离: \n \
            位置1~4: 分别表示是否MACD底背离、DIFF底背离、MACD柱背离、MACD柱面积底背离（1: 是, 0:否） \n \
            位置5~8: 分别表示是否MACD顶背离、DIFF顶背离、MACD顶背离、MACD柱面积顶背离（1: 是, 0:否） \n \
        3、分型: \n \
            位置1: 是否有顶分型or底分型（1: 是, 0:否） \n \
            位置2: 是否顶分型（1: 是, 0:否） \n \
            位置3: 是否底分型（1: 是, 0:否） \n \
        4、ADX极值: \n \
            位置1: adx是否上下穿过60&-60（0: 非极值点, 1: 上穿-60.0, -1: 下穿60）\
        '
        fu.save_xlsx(os.path.join(out_path, file_name + '.xlsx'), res_arr.T.tolist(), tips=tips)

