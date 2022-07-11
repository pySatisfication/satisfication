import sys

KTIME_TEN_FOURTEEN_FOURTYFIVE   = '10:14:45'
KTIME_TEN_FOURTEEN_THIRTY       = '10:14:30'

KTIME_NINE                      = '09:00'
KTIME_NINE_THIRTY               = '09:30'
KTIME_TEN                       = '10:00'
KTIME_TEN_THIRTY                = '10:30'
KTIME_TEN_FOURTYFIVE            = '10:45'
KTIME_ELEVEN                    = '11:00'
KTIME_ELEVEN_FIFTEEN            = '11:15'
KTIME_THIRTEEN                  = '13:00'
KTIME_THIRTEEN_THIRTY           = '13:30'
KTIME_THIRTEEN_FORTYFIVE        = '13:45'
KTIME_FOURTEEN                  = '14:00'
KTIME_FOURTEEN_FIFTEEN          = '14:15'
KTIME_FOURTEEN_THIRTY           = '14:30'
KTIME_FOURTEEN_FORTYFIVE        = '14:45'
KTIME_FIFTEEN                   = '15:00'
KTIME_TWENTYONE                 = '21:00'
KTIME_TWENTYONE_THIRTY          = '21:30'
KTIME_TWENTYTWO                 = '22:00'
KTIME_TWENTYTWO_THIRTY          = '22:30'
KTIME_TWENTYTHREE               = '23:00'
KTIME_TWENTYTHREE_THIRTY        = '23:30'
KTIME_ZERO                      = '00:00'
KTIME_ZERO_THIRTY               = '00:30'
KTIME_ONE                       = '01:00'
KTIME_ONE_THIRTY                = '01:30'
KTIME_TWO                       = '02:00'

TIME_THREE                  = '03:00:00'
TIME_NINE                   = '09:00:00'
TIME_NINE_THIRTY            = '09:30:00'
TIME_TEN                    = '10:00:00'
TIME_TEN_FIFTEEN            = '10:15:00'
TIME_TEN_THIRTY             = '10:30:00'
TIME_TEN_FORTYFIVE          = '10:45:00'
TIME_ELEVEN                 = '11:00:00'
TIME_ELEVEN_FIFTEEN         = '11:15:00'
TIME_ELEVEN_THIRTY          = '11:30:00'
TIME_TWELVE                 = '12:00:00'
TIME_THIRTEEN_THIRTY        = '13:30:00'
TIME_THIRTEEN_FORTYFIVE     = '13:45:00'
TIME_FOURTEEN               = '14:00:00'
TIME_FOURTEEN_FIFTEEN       = '14:15:00'
TIME_FOURTEEN_THIRTY        = '14:30:00'
TIME_FOURTEEN_FORTYFIVE     = '14:45:00'
TIME_FIFTEEN                = '15:00:00'
TIME_FIFTEEN_FIFTEEN        = '15:15:00'
TIME_TWENTYONE              = '21:00:00'
TIME_TWENTYONE_THIRTY       = '21:30:00'
TIME_TWENTYTWO              = '22:00:00'
TIME_TWENTYTWO_THIRTY       = '22:30:00'
TIME_TWENTYTHREE            = '23:00:00'
TIME_TWENTYTHREE_THIRTY     = '23:30:00'
TIME_TWENTYTHREE_FIFTYNINE  = '23:59:00'
TIME_ZERO_THIRTY            = '00:30:00'
TIME_ZERO                   = '00:00:00'
TIME_ZERO_2                 = '24:00:00'
TIME_ONE                    = '01:00:00'
TIME_ONE_THIRTY             = '01:30:00'
TIME_TWO                    = '02:00:00'
TIME_TWO_THIRTY             = '02:30:00'

# 10:15, 11:30, 23:00, 01:00, 02:30
TIME_TEN_SIXTEEN            = '10:15:59'
TIME_ELEVEN_THIRTYONE       = '11:30:59'
TIME_FIFTEEN_ONE            = '15:00:59'
TIME_FIFTEEN_SIXTEEN        = '15:15:59'
TIME_FIFTEEN_THIRTY         = '15:30:00'
TIME_TWENTYTHREE_ONE        = '23:00:59'
TIME_ONE_ONE                = '01:00:59'
TIME_TWO_THIRTYONE          = '02:30:59'

OPEN_TIME1 = '09:00:00'
OPEN_TIME2 = '09:30:00'
OPEN_TIME3 = '13:30:00'
OPEN_TIME4 = '21:00:00'

# special time
CLOSE_TIME1 = '10:15:00'
CLOSE_TIME2 = '11:30:00'
CLOSE_TIME3 = '15:00:00'
CLOSE_TIME4 = '15:15:00'
CLOSE_TIME5 = '23:00:00'
CLOSE_TIME6 = '01:00:00'
CLOSE_TIME7 = '02:30:00'
CLOSE_TIMES = [CLOSE_TIME1, CLOSE_TIME2, 
               CLOSE_TIME3, CLOSE_TIME4, 
               CLOSE_TIME5, 
               CLOSE_TIME6, CLOSE_TIME7]

class TranTimeHelper(object):
    def __init__(self):
        self.conf_file = './tran_time_conf'
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

