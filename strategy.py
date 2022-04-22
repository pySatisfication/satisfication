import sys
import time
import json
import threading
import logging
import contract as ct
import utils.redis_ts_util as rtu

class ContractFactory(object):
    def __init__(self):
        pass

    @classmethod
    def c_creator(cls, m_code):
        if m_code.startswith('IC'):
            return ct.Option(m_code)
        else:
            pass

class BaseStrategy(object):
    def __init__(self, stg_name):
        self._name = stg_name

    def step(self, *args, **kwargs):
        """
        typical entrance of running for quantitative strategy
        """
        raise NotImplementedError()

class SimpleStrategy(BaseStrategy):
    def __init__(self, name, initializer=None):
        super(SimpleStrategy, self).__init__(name)
        self._ct_lock = threading.Lock()
        self._log = logging.getLogger(u".%s" % name)
        self.init_data()

    def init_data(self):
        self._ct_data = {}

    def step(self, *args, **kwargs):
        self._ct_lock.acquire()
        data = args[0]
        c_code = data.c_code

        #self.log.info(u"consume item: %s", item.time)
        if c_code in self._ct_data:
            item = self._ct_data[c_code]
        else:
            item = ContractFactory.c_creator(c_code)
            self._ct_data[c_code] = item

        # step 1. update transaction data and base indicators
        j_idx_str = item.iterate(data)
        try:
            # save in time-series for current step
            if rtu.add(c_code, data.time, j_idx_str):
                print('write intermediate results to redis successfully:', data.time)
            else:
                print('write intermediate results to redis failed:', data.time)
        except ValueError as e:
            print(e)
        
        # step 2. update strategy results
        # macd
        f_dev = item.check_macd_dev()
        # boll
        f_boll = item.check_boll()
        # dmi
        f_dmi = item.check_dmi()
        # parting
        f_par = item.check_parting()

        j_stg_str = json.dumps({'f_dev': ','.join(f_dev),
            'f_boll': ','.join(f_boll),
            'f_dmi': ','.join(f_dmi),
            'f_par': ','.join(f_par)})
        try:
            # save in time-series for current step
            if rtu.add(c_code + ':stg', data.time, j_stg_str):
                print('write stg results to redis successfully:', data.time)
            else:
                print('write stg results to redis failed:', data.time)
        except ValueError as e:
            print(e)

        self._ct_lock.release()

    def stg_boll_check(self, obj):
        return obj.check_boll()

    def stg_dmi_check(self, obj):
        return obj.check_dmi()
    
    def stg_parting_check(self, obj):
        return obj.check_parting()

    def stg_macd_check(self, obj):
        """
        step strategy
        """
        # check devergence
        f_dev = obj.check_macd_dev()
        if f_dev[0]:
            # TODO GUI
            print('MACD bottom divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[1]:
            print('diff bottom divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[2]:
            print('MACD column bottom divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[3]:
            print('area of MACD bottom divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[4]:
            print('MACD top divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[5]:
            print('diff top divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[6]:
            print('MACD column top divergence', [item.print() for item in obj.inters[-4:]])
        if f_dev[7]:
            print('area of MACD column top divergence', [item.print() for item in obj.inters[-4:]])

