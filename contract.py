import json
from easydict import EasyDict as edict

import utils.util as op_util
import utils.redis_ts_util as rtu

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

class Intersection(object):
    def __init__(self, t_inter, start, end, close_idx):
        self._type = t_inter
        self._start_time = start
        self._end_time = end
        self._close_idx = close_idx
        
    @property
    def close_idx(self):
        return self._close_idx

    @property
    def t_inter(self):
        return self._type

    def print(self):
        return 'type:{},start:{},end:{},close_idx:{}'.format(self._type, self._start_time, self._end_time, self._close_idx)

class Contract(object):
    def __init__(self, m_code):
        self._m_code = m_code

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

        # macd
        self._diff = []
        self._dea = []
        self._macd = []

        self._inters = []

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

        # last_time
        self._time = []

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

    def clct_all_var(self):
        m_var = {}
        for k, v in self.__dict__.items():
            if not isinstance(v, list) or len(v) == 0:
                continue
            if k == '_inters':
                continue
            m_var[k] = v[-1]
        return json.dumps(m_var)

class Option(Contract):
    def __init__(self, m_code):
        assert m_code is not None
        super(Option, self).__init__(m_code=m_code)
        self._type = 'option'

    @property
    def m_code(self):
        return self.m_code

    def update_trans(self, item):
        assert item.open is not None and isinstance(item.open, float)
        assert item.high is not None and isinstance(item.high, float)
        assert item.low is not None and isinstance(item.low, float)
        assert item.close is not None and isinstance(item.close, float)

        self._open.append(item.open)
        if len(self._open) > _RESERVE_DAYS:
            self._open = self._open[-_RESERVE_DAYS:]

        self._high.append(item.high)
        if len(self._high) > _RESERVE_DAYS:
            self._high = self._high[-_RESERVE_DAYS:]

        self._low.append(item.low)
        if len(self._low) > _RESERVE_DAYS:
            self._low = self._low[-_RESERVE_DAYS:]

        self._close.append(item.close)
        if len(self._close) > _RESERVE_DAYS:
            self._close = self._close[-_RESERVE_DAYS:]

        self._volumes.append(item.volumes)
        if len(self._volumes) > _RESERVE_DAYS:
            self._volumes = self._volumes[-_RESERVE_DAYS:]

        self._holds.append(item.holds)
        if len(self._holds) > _RESERVE_DAYS:
            self._holds = self._holds[-_RESERVE_DAYS:]

        self._amounts.append(item.amounts)
        if len(self._amounts) > _RESERVE_DAYS:
            self._amounts = self._amounts[-_RESERVE_DAYS:]

        self._avg_prices.append(item.avg_prices)
        if len(self._avg_prices) > _RESERVE_DAYS:
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

        if len(self._ema12) > _RESERVE_DAYS:
            self._ema12 = self._ema12[-_RESERVE_DAYS:]

        # 26 days
        cur_val = op_util.ema_cc(self._ema26, self._close[-1], _EMA26_DEFAULT_N)
        self._ema26.append(round(cur_val, 2))

        if len(self._ema26) > _RESERVE_DAYS:
            self._ema26 = self._ema26[-_RESERVE_DAYS:]

    def update_diff(self):
        assert len(self._ema12) > 0 and len(self._ema26) > 0
        cur_diff = self._ema12[-1] - self._ema26[-1]
        self._diff.append(cur_diff)

        if len(self._diff) > _RESERVE_DAYS:
            self._diff = self._diff[-_RESERVE_DAYS:]

    def update_dea(self):
        assert len(self._diff) > 0

        cur_val = op_util.ema_cc(self._dea, self._diff[-1], _DEA_DEFAULT_N)
        self._dea.append(round(cur_val, 2))

        if len(self._dea) > _RESERVE_DAYS:
            self._dea = self._dea[-_RESERVE_DAYS:]

    def update_macd(self):
        assert len(self._diff) > 0 and len(self._dea) > 0
        cur_val = 2*(self._diff[-1] - self._dea[-1])
        self._macd.append(cur_val)
        if len(self._macd) > _RESERVE_DAYS:
            self._macd = self._macd[-_RESERVE_DAYS:]

    # boll
    def update_boll(self, N=20, M=2):
        """
        calculate boll in 3 types
        """
        if len(self._close) < N:
            N = len(self._close)

        # make sure cur price has been updated
        cur_boll_st = op_util.ma(self._close, N)
        self._boll_st.append(cur_boll_st)
        if len(self._boll_st) > _RESERVE_DAYS:
            self._boll_st = self._boll_st[-_RESERVE_DAYS:]

        cur_ub_st = cur_boll_st + M * op_util.std(self._close, N)
        self._ub_st.append(cur_ub_st)
        if len(self._ub_st) > _RESERVE_DAYS:
            self._ub_st = self._ub_st[-_RESERVE_DAYS:]

        cur_lb_st = cur_boll_st - M * op_util.std(self._close, N)
        self._lb_st.append(cur_lb_st)
        if len(self._lb_st) > _RESERVE_DAYS:
            self._lb_st = self._lb_st[-_RESERVE_DAYS:]

        cur_gl = round((self._high[-1] + self._low[-1] + self._open[-1] + 3 * self._close[-1]) / 6.0, 2)
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

        self._boll_st_s2.append(op_util.ma(self._boll_st_s1, N))
        if len(self._boll_st_s2) > _RESERVE_DAYS:
            self._boll_st_s2 = self._boll_st_s2[-_RESERVE_DAYS:]

        self._boll_st_s3.append(op_util.ma(self._boll_st_s2, N))
        if len(self._boll_st_s3) > _RESERVE_DAYS:
            self._boll_st_s3 = self._boll_st_s3[-_RESERVE_DAYS:]

        self._boll_st_s4.append(op_util.ma(self._boll_st_s3, N))
        if len(self._boll_st_s3) > _RESERVE_DAYS:
            self._boll_st_s4 = self._boll_st_s4[-_RESERVE_DAYS:]

        self._boll_st_s5.append(op_util.ma(self._boll_st_s4, N))
        if len(self._boll_st_s5) > _RESERVE_DAYS:
            self._boll_st_s5 = self._boll_st_s5[-_RESERVE_DAYS:]

    def update_dmi(self):
        """
        first item in tr is max(hl, hr, lr), the following elements 
        are calulated by SMA(X,N,M)
        """
        if len(self._close) < 2 \
            or len(self._high) < 2 \
            or len(self._low) < 2:
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

        if len(self._tr) > _RESERVE_DAYS:
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

    def check_macd_dev(self, item):
        """
        parameters:
            item    // transcation info
        return:
            an array with length of 8,
            index 0~3       // bottom divergence
            index 4~7       // top divergence
        """
        f_dev = [0]*8
        if len(self._diff) == 1:
            return f_dev

        if self._diff[-2] < self._dea[-2] and self._diff[-1] > self._dea[-1]:     # jincha
            # 1. record
            self._inters.append(Intersection('K', self._time[-1], item.time, len(self._close) - 1))
            if len(self._inters) < 4:
                return f_dev 

            # 2. bottom divergence
            assert self._inters[-2].t_inter == 'D' \
                and self._inters[-3].t_inter == 'K' \
                and self._inters[-4].t_inter == 'D'

            # [s, e)
            c_max1 = max(self._close[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            c_max2 = max(self._close[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if c_max1 <= c_max2:
                f_dev[0] = 1

            diff_max1 = max(self._diff[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            diff_max2 = max(self._diff[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if diff_max1 > diff_max2:
                f_dev[1] = 1

            macd_max1 = max(self._macd[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            macd_max2 = max(self._macd[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if macd_max1 > macd_max2:
                f_dev[2] = 1

            macd_area1 = sum([abs(m) for m in self._macd[self._inters[-2].close_idx:(self._inters[-1].close_idx)]])
            macd_area2 = sum([abs(m) for m in self._macd[self._inters[-4].close_idx:(self._inters[-3].close_idx)]])
            if macd_area1 > macd_area2:
                f_dev[3] = 1
        elif self._diff[-2] > self._dea[-2] and self._diff[-1] < self._dea[-1]:   # sicha
            # 1. record
            self._inters.append(Intersection('D', self._time[-1], item.time, len(self._close) - 1))
            if len(self._inters) < 4:
                return f_dev

            # 2. top divergence
            assert self._inters[-2].t_inter == 'K' \
                and self._inters[-3].t_inter == 'D' \
                and self._inters[-4].t_inter == 'K'

            # [s, e)
            c_max1 = max(self._close[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            c_max2 = max(self._close[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if c_max1 >= c_max2:
                f_dev[4] = 1

            diff_max1 = max(self._diff[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            diff_max2 = max(self._diff[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if diff_max1 < diff_max2:
                f_dev[5] = 1

            macd_max1 = max(self._macd[self._inters[-2].close_idx:(self._inters[-1].close_idx)])
            macd_max2 = max(self._macd[self._inters[-4].close_idx:(self._inters[-3].close_idx)])
            if macd_max1 < macd_max2:
                f_dev[6] = 1

            macd_area1 = sum([abs(m) for m in self._macd[self._inters[-2].close_idx:(self._inters[-1].close_idx)]])
            macd_area2 = sum([abs(m) for m in self._macd[self._inters[-4].close_idx:(self._inters[-3].close_idx)]])
            if macd_area1 < macd_area2:
                f_dev[7] = 1
        return f_dev

    def iterate(self, item):
        assert isinstance(item, dict)
        if len(self._time) > 0:
            assert item.time > self._time[-1], \
                'time of item must be greater than lastest updated time:{}'.format(
                self._lastest_update_time)
        """
        step 1. update indicator
        """

        self.update_trans(item)
        self.update_ema()
        self.update_boll()
        self.update_guppyline()
        #self.update_parting()

        self.update_diff()
        self.update_dea()
        self.update_macd()

        self.update_dmi()

        """
        step 2. strategy
        """

        # check devergence
        f_dev = self.check_macd_dev(item)
        if f_dev[0]:
            # TODO GUI
            print('MACD bottom divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[1]:
            print('diff bottom divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[2]:
            print('MACD column bottom divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[3]:
            print('area of MACD bottom divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[4]:
            print('MACD top divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[5]:
            print('diff top divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[6]:
            print('MACD column top divergence', [item.print() for item in self._inters[-4:]])
        if f_dev[7]:
            print('area of MACD column top divergence', [item.print() for item in self._inters[-4:]])

        # self.check_boll()

        # debug ema
        if item.time == '202110181459':
            print('final close:', self._close)
            print('final ema12:', self._ema12)

        self._time.append(item.time)

        # save data for current step
        try:
            j_data = self.clct_all_var()
            if rtu.add(self._m_code, item.time, j_data):
                print('write to redis successfully:', item.time)
        except ValueError as e:
            print(e)

if __name__ == '__main__':
    cc = Option('code_3')
    t_data = ['3,IC2206,202110180929,6679.0,6679.0,6679.0,6679.0,3,3,20037.0,6679.0',
              '3,IC2206,202110180930,6666.6,6688.8,6666.0,6675.0,32,34,213660.00000000003,6676.875000000001',
              '3,IC2206,202110180931,6673.8,6675.4,6660.0,6660.0,33,67,220091.59999999998,6669.442424242424',
              '3,IC2206,202110180932,6661.8,6661.8,6650.0,6651.8,20,87,133101.4,6655.07',
              '3,IC2206,202110180933,6650.0,6655.2,6647.0,6647.2,28,115,186226.6,6650.95',
              '3,IC2206,202110180934,6647.6,6654.6,6647.6,6654.0,11,126,73167.0,6651.545454545455',
              '3,IC2206,202110180935,6649.6,6665.8,6649.4,6659.6,18,144,119841.20000000001,6657.844444444445',
              '3,IC2206,202110180936,6662.2,6664.6,6648.8,6652.6,28,172,186382.20000000004,6656.507142857145',
              '3,IC2206,202110180937,6654.4,6660.4,6650.4,6659.2,30,202,199699.00000000003,6656.633333333334',
              '3,IC2206,202110180938,6666.0,6666.0,6645.0,6645.0,27,229,179653.2,6653.822222222223',
              '3,IC2206,202110180939,6647.0,6652.4,6640.0,6640.0,21,250,139579.0,6646.619047619048',
              '3,IC2206,202110180940,6640.0,6655.4,6639.8,6650.4,40,290,265889.99999999994,6647.249999999998',
              '3,IC2206,202110180941,6655.2,6655.2,6643.4,6649.8,35,325,232663.79999999996,6647.537142857142',
              '3,IC2206,202110180942,6645.2,6648.8,6641.0,6642.8,49,374,325595.3999999999,6644.804081632651',
              '3,IC2206,202110180943,6639.8,6645.6,6639.2,6645.6,29,402,192609.2,6641.696551724139']

    for line in t_data:
        items = line.split(',')
        d_data = edict(m_code = int(items[0]), 
                       c_code = items[1],
                       time = items[2],
                       open = float(items[3]),
                       high = float(items[4]),
                       low = float(items[5]),
                       close = float(items[6]),
                       volumes = float(items[7]),
                       holds = float(items[8]),
                       amounts = float(items[9]),
                       avg_prices = float(items[10])
                    )
        cc.iterate(d_data)
        res = cc.clct_all_var()
        j_res = json.loads(res)
        for k, v in j_res.items():
            print(k + '\t' + str(v))

