import sys
import time
import json
import threading
import logging
import contract as ct
import utils.redis_util as ru
from easydict import EasyDict as edict

class ContractFactory(object):
    def __init__(self):
        pass

    @classmethod
    def c_creator(cls, m_code):
        return ct.Option(m_code)
        #if m_code.startswith('LH'):
        #    return ct.Option(m_code)
        #else:
        #    pass

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
        #self._ct_lock.acquire()
        data = args[0]
        c_code = data.code
        period = data.period_type

        #self.log.info(u"consume item: %s", item.time)
        if c_code in self._ct_data:
            if period in self._ct_data[c_code]:
                item = self._ct_data[c_code][period]
            else:
                item = ContractFactory.c_creator(c_code)
                self._ct_data[c_code][period] = item
        else:
            item = ContractFactory.c_creator(c_code)
            #logger.info('[strategy]new contract: {}'.format(c_code))
            self._ct_data[c_code] = {period : item}

        d_item = edict(m_code='0',
                       code=data.code,
                       time=data.k_time,
                       open=float(data.open),
                       high=float(data.high),
                       low=float(data.low),
                       close=float(data.close),
                       volume=float(data.volume),
                       open_interest=float(data.open_interest),
                       amounts=float(data.turnover),
                       period=data.period_type)

        # step 1. update transaction data and base indicators
        #print(self._ct_data)
        j_idx_str = item.iterate(d_item)
        item.make_judge()
        #try:
        #    # save in time-series for current step
        #    if ru.add(c_code + '_' + data.period, data.time, j_idx_str):
        #        print('write intermediate results to redis successfully:', data.time)
        #    else:
        #        print('write intermediate results to redis failed:', data.time)
        #except ValueError as e:
        #    print(e)
        
        '''
        # step 2. update strategy results
        # macd
        f_dev, f_sep_dev, f_in_dev, f_in_sep_dev, f_cross_valid = item.check_dev()
        # boll
        f_boll = item.check_boll()
        # dmi
        f_dmi = item.check_dmi()
        # parting
        f_par = item.check_parting()

        j_stg_str = json.dumps({'f_dev': ''.join(f_dev),
            'f_sep_dev': ''.join(f_sep_dev),
            'f_in_dev': ''.join(f_in_dev),
            'f_in_sep_dev': ''.join(f_in_sep_dev),
            'f_cross_valid': ''.join(f_cross_valid),
            'f_boll': ','.join(f_boll),
            'f_dmi': ','.join(f_dmi),
            'f_par': ''.join(f_par)})
        try:
            # save in time-series for current step
            if ru.add(c_code + '_' + data.period + ':stg', data.time, j_stg_str):
                print('write stg results to redis successfully:', data.time)
            else:
                print('write stg results to redis failed:', data.time)
        except ValueError as e:
            print(e)
        '''

        #self._ct_lock.release()

        return j_idx_str

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
        f_dev, f_inter_dev = obj.check_dev()
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

