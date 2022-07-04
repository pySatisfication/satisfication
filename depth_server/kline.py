
class KLine(object):
    def __init__(self, data, sys_time=None):
        self._code = data[0]
        self._period_type = data[1]
        self._k_time = data[2]
        self._open = data[3]
        self._high = data[4]
        self._low = data[5]
        self._close = data[6]
        self._volume = data[7]
        self._open_interest = data[8]
        self._turnover = data[9]
        if sys_time:
            self._sys_time = sys_time

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, code):
        self._code = code

    @property
    def period_type(self):
        return self._period_type

    @period_type.setter
    def period_type(self, period_type):
        self._period_type = period_type

    @property
    def k_time(self):
        return self._k_time

    @k_time.setter
    def k_time(self, k_time):
        self._k_time = k_time

    @property
    def open(self):
        return self._open

    @open.setter
    def open(self, open):
        self._open = open

    @property
    def high(self):
        return self._high

    @high.setter
    def high(self, high):
        self._high = high

    @property
    def low(self):
        return self._low

    @low.setter
    def low(self, low):
        self._low = low

    @property
    def close(self):
        return self._close

    @close.setter
    def close(self, close):
        self._close = close

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, volume):
        self._volume = volume

    @property
    def open_interest(self):
        return self._open_interest

    @open_interest.setter
    def open_interest(self, open_interest):
        self._open_interest = open_interest

    @property
    def turnover(self):
        return self._turnover

    @turnover.setter
    def turnover(self, turnover):
        self._turnover = turnover

    def __repr__(self):
        return "{},{},{},{},{},{},{},{},{},{}".format(
            self._code, self._period_type, self._k_time,
            self._open, self._high, self._low, self._close,
            self._volume, self._open_interest, self._turnover)

    def print_line(self):
        return "{},{},{},{},{},{},{},{},{},{}".format(
            self._code, self._period_type, self._k_time,
            self._open, self._high, self._low, self._close,
            self._volume, self._open_interest, self._turnover)

