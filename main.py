# coding=utf-8
import sys
import time
import threading
import argparse
import logging
from easydict import EasyDict as edict

import proconex as px
import contract as ct
import util as op_util

_PRODUCTION_DELAY = 0.05
_CONSUMPTION_DELAY = 0.05
_DEFAULT_PRODUCER_NUM = 1
_DEFAULT_CONSUMER_NUM = 1

_ct_lock = threading.Lock()
_ct_data = {}

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='arguments for modular call')
    parser.add_argument('--trans_data_path', dest='data_path', help='transaction data file',
                        default='trans_data.txt', type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

"""
transaction data in memory
==========================

trans_d = {
    'm_code' : {
        #base
        'open' : [],
        'high' : [],
        'low' : [],
        'close' : [],
        #ma
        'ma' : [], 
        'sma' : [], 
        'ema' : [], 
        'std' : [], 
        #boll
        'boll_st' : [], 
        'ub_st' : [], 
        'lb_st' : [], 
    },
}
"""

class TransDProducer(px.Producer):
    def __init__(self, name, fp, itemProducerFailsAt=None):
        assert name
        super(TransDProducer, self).__init__(name)
        self._file_path = fp

    def items(self):
        self.log.info(u"producing items...")

        with open(self._file_path, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue

                d_singl = line.split(',')
                if len(d_singl) != 11:
                    self.log.warning(u"line error: %s", line)
                    continue

                d_singl = edict(m_code = int(d_singl[0]), 
                                c_code = d_singl[1],
                                time = d_singl[2],
                                open = float(d_singl[3]),
                                high = float(d_singl[4]),
                                low = float(d_singl[5]),
                                close = float(d_singl[6]),
                                volumes = float(d_singl[7]),
                                holds = float(d_singl[8]),
                                amounts = float(d_singl[9]),
                                avg_prices = float(d_singl[10])
                            )
                #time.sleep(_PRODUCTION_DELAY)

                self.log.info('queue size: %s', self.workEnv.queue.qsize())
                yield d_singl

class TransDConsumer(px.Consumer):
    def __init__(self, name):
        assert name
        super(TransDConsumer, self).__init__(name)

    def consume(self, item):
        _ct_lock.acquire()

        #self.log.info(u"consume item: %s", item.time)
        #self.log.info(u"item type: %s", item.c_code)
        if item.c_code in _ct_data:
            option = _ct_data[item.c_code]
        else:
            option = ct.Option(item.c_code)
            _ct_data[item.c_code] = option
        option.iterate(item)
        #time.sleep(_CONSUMPTION_DELAY)

        _ct_lock.release()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    print('Called with args:')
    print(args)

    # create producer and consumers
    producers = []
    for producerId in range(_DEFAULT_PRODUCER_NUM):
        producerToStart = TransDProducer(u"producer.%d" % producerId, fp=args.data_path)
        producers.append(producerToStart)

    consumers = []
    for consumerId in range(_DEFAULT_CONSUMER_NUM):
        consumerToStart = TransDConsumer(u"consumer.%d" % consumerId)
        consumers.append(consumerToStart)

    worker = px.Worker(producers, consumers, 1000)
    worker.work()

    print()
    print('============EMA(N=12)==============') 
    X = _ct_data['IC2206'].close
    for day in range(len(X)):
        print("day {}: non-recursion: {}".format(day+1, op_util.ema(X[:day+1], 12)))

