import sys
import time
import threading
import contract as ct

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
        self.init_data()

    def init_data(self):
        self._ct_data = {}

    def step(self, *args, **kwargs):
        self._ct_lock.acquire()
        item = args[0]

        #self.log.info(u"consume item: %s", item.time)
        if item.c_code in self._ct_data:
            option = self._ct_data[item.c_code]
        else:
            option = ct.Option(item.c_code)
            self._ct_data[item.c_code] = option
        option.iterate(item)
        #time.sleep(_CONSUMPTION_DELAY)

        self._ct_lock.release()
        
