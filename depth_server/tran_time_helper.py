import sys

class TranTimeHelper(object):
    def __init__(self):
        self.conf_file = '../conf/tran_time_conf'
        self._code_time_spans = {}
        self._code_open_times = {}
        self._code_close_times = {}
        self._code_morning_suspend = {}
        self.init_spans()

    def close_times(self):
        return self._code_close_times

    def open_times(self):
        return self._code_open_times

    def init_spans(self):
        with open(self.conf_file, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue

                items = line.split('\t')
                assert len(items) > 6
                code_prefix = items[0]

                tmp = []
                tmp.append([time_span.split(',') for time_span in items[1].strip().split(' ')])
                tmp.append([time_span.split(',') for time_span in items[2].strip().split(' ')])
                tmp.append([time_span.split(',') for time_span in items[3].strip().split(' ')])
                self._code_time_spans[code_prefix] = tmp

                self._code_open_times[code_prefix] = items[4].strip().split(',')
                self._code_close_times[code_prefix] = items[5].strip().split(',')
                self._code_morning_suspend[code_prefix] = items[6].strip()

    def check_open_time(self, code_prefix, time):
        return code_prefix in self._code_open_times \
            and time in self._code_open_times[code_prefix]

    def check_close_time(self, code_prefix, time):
        return code_prefix in self._code_close_times \
            and time in self._code_close_times[code_prefix]

    def check_morning_suspend(self, code_prefix):
        return code_prefix in self._code_morning_suspend \
            and self._code_morning_suspend[code_prefix] == '1'

    def get_open_times(self, code_prefix):
        if code_prefix in self._code_open_times:
            return self._code_open_times[code_prefix]
        return []

    def get_nearest_open(self, code_prefix, update_time):
        if code_prefix not in self._code_open_times:
            return None
        times = self._code_open_times[code_prefix]
        c_hour = int(update_time[0:2])
        for time in times:
            t_hour =  int(time[0:2])
            if abs(t_hour - c_hour) <= 1:
                return time
        return None

    def get_code_prefix(self, code):
        prefix = ''
        for letr in code:
            if (letr <= 'z' and letr >= 'a') or (letr <= 'Z' and letr >= 'A'):
                prefix += letr
            else:
                break
        return prefix

    def check_in(self, t_type, code, time):
        '''
        t_type:
            0: 正常交易时间
            1: 非正常交易时间
            2: 集合竞价时间
        '''
        prefix = self.get_code_prefix(code.strip())

        time = ':'.join(time.split(':')[0:2])

        spans = self._code_time_spans[prefix][t_type]
        for span in spans:
            if t_type == 0 and time >= span[0] and time <= span[1]:
                return True
            if t_type == 1 and time > span[0] and time < span[1]:
                return True
            if t_type == 2 and time >= span[0] and time <= span[1]:
                return True
        return False

