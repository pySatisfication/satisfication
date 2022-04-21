import sys
import time
import threading
import logging
import contract as ct

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
        item.iterate(data)
        #time.sleep(_CONSUMPTION_DELAY)
        
        # macd
        self.stg_macd_check(item)
        # boll
        self.stg_boll_check(item)

        self._ct_lock.release()

    def stg_boll_check(self, obj):
        pass
    
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

