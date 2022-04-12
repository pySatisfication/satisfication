import util as op_util

_RESERVE_DAYS = 3000
_EMA12_DEFAULT_N = 12
_EMA26_DEFAULT_N = 26
_DEA_DEFAULT_N = 9

class Contract(object):
    def __init__(self, m_code):
        self._m_code = m_code

        # base
        self._open = []
        self._high = []
        self._low = []
        self._close = []
        # ma
        self._ma = []
        self._ema = []
        self._ema12 = []
        self._ema26 = []
        self._sma = []
        # std
        self._std = []

        # macd
        self._diff = []
        self._dea = []
        self._macd = []

        # boll 
        self._boll_st = []
        self._ub_st = []
        self._lb_st = []

        # last_time
        self._lastest_update_time = '19700101'

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

class Option(Contract):
    def __init__(self, m_code):
        assert m_code is not None
        super(Option, self).__init__(m_code=m_code)

    @property
    def m_code(self):
        return self.m_code

    def update_price(self, item):
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

    def update_ema(self):
        """
        calculate ema in sevaral days
        """
        assert len(self._close) > 0
        # ema in 12 days
        if len(self._ema12) == 0:
            self._ema12.append(self._close[0])
        else:
            cur_val = op_util.ema_alge(self._ema12[-1], self._close[-1], _EMA12_DEFAULT_N)
            self._ema12.append(round(cur_val, 2))

        if len(self._ema12) > _RESERVE_DAYS:
            self._ema12 = self._ema12[-_RESERVE_DAYS:]

        # 26 days
        if len(self._ema26) == 0:
            self._ema26.append(self._close[0])
        else:
            cur_val = op_util.ema_alge(self._ema26[-1], self._close[-1], _EMA26_DEFAULT_N)
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

        if len(self._dea) == 0:
            self._dea.append(self._diff[-1])
        else:
            cur_val = op_util.ema_alge(self._dea[-1], self._diff[-1], _DEA_DEFAULT_N)
            self._dea.append(cur_val)

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

    def iterate(self, item):
        assert isinstance(item, dict)
        assert item.time > self._lastest_update_time, \
            'time of item must be greater than lastest updated time:{}'.format(
            self._lastest_update_time)

        self.update_price(item)
        self.update_ema()
        self.update_boll()

        self.update_diff()
        self.update_dea()
        self.update_macd()

        ## TODO strategy
        # self.check_devi()
        # self.check_boll()

        if item.time == '202110181459':
            print('final close:', self._close)
            print('final ema12:', self._ema12)

        self._lastest_update_time = item.time
        
