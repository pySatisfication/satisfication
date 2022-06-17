# coding=utf-8
import os
import sys
import time
import argparse
import logging
from easydict import EasyDict as edict

import strategy as stg
import parallel as px
import utils.util as op_util
import utils.file_util as futil

_PRODUCTION_DELAY = 0.05
_CONSUMPTION_DELAY = 0.05
_DEFAULT_PRODUCER_NUM = 1
_DEFAULT_CONSUMER_NUM = 3

M_PD_BUCKET = {
    # day
    '4_1':1, 
    '4_3':1, 
    '4_5':1, 
    '4_20':1,
    # hour
    '5_1':1,
    '5_2':1,
    '5_3':1,
    '5_5':1,
    # minute
    '6_1':2,
    '6_5':1,
    '6_15':1,
    '6_30':1,
    '6_45':1,
    # second
    '7_15':5,
    '7_30':3,
}

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

class TransDProducer(px.Producer):
    def __init__(self, name, fp, itemProducerFailsAt=None):
        assert name
        super(TransDProducer, self).__init__(name)
        self._file_path = fp
        self._m_ccode_id = {}

    def get_pd_type(self, file_path):
        file_name = file_path.split('/')[-1]
        assert file_name is not None and len(file_name) > 0
        file_name = file_name.split('.')[0]
        assert file_name is not None and len(file_name) > 0
        p_types = file_name.split('_')
        assert len(p_types) == 3

        return p_types[1] + '_' + p_types[2]

    def read_offline_data(self, file_path, pd_type):
        assert file_path

        data = []
        with open(file_path, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue

                items = line.split(',')
                if len(items) != 11:
                    self.log.warning("line error: %s", line)
                    continue

                d_item = edict(m_code = int(items[0]), 
                                c_code = items[1],
                                time = items[2],
                                open = float(items[3]),
                                high = float(items[4]),
                                low = float(items[5]),
                                close = float(items[6]),
                                volumes = float(items[7]),
                                holds = float(items[8]),
                                amounts = float(items[9]),
                                avg_prices = float(items[10]),
                                period = pd_type
                            )
                data.append(d_item)
        return data

    def init_start_bid(self):
        sort_arr = sorted(M_PD_BUCKET.items(), key=lambda x:x[1], reverse=False)
        bucket = 0
        for i, item in enumerate(sort_arr):
            k, v = item
            bucket += v
            if i == 0:
                self.m_start_bid[k] = 0
            else:
                self.m_start_bid[k] = bucket - v

    def items(self):
        self.log.info("producing items...")
        
        # init start bucketID for each period data
        self.init_start_bid()

        ct_dirs = futil.get_sub_dirs(self._file_path)
        all_data = []
        for ct_dir in ct_dirs:
            if ct_dir != 'LH2207':
                continue
            p_dirs = futil.get_sub_dirs(os.path.join(self._file_path, ct_dir))

            for p_dir in p_dirs:
                if p_dir not in ('3', '4'):
                    continue
                # 4:day, 5:hour, 6:minute, 7:second
                p_files = sorted(futil.get_all_files(os.path.join(self._file_path, ct_dir, p_dir), 'spt'))
                #print(ct_dir, ', files:', len(p_files))
                d_cnt = 0
                for p_file in p_files:
                    #print('produced file:', p_file)

                    pd_type = self.get_pd_type(p_file)
                    if pd_type not in M_PD_BUCKET:
                        #print('not in .............:', pd_type)
                        continue

                    # buckets num
                    buckets = M_PD_BUCKET[pd_type]

                    if ct_dir in self._m_ccode_id:
                        b_offset = self._m_ccode_id[ct_dir]
                    else:
                        b_offset = len(self._m_ccode_id)
                        self._m_ccode_id[ct_dir] = b_offset

                    b_id = b_offset % buckets + self.m_start_bid[pd_type]
                    #print('pd_type: %s, ct: %s, bucket_id: %s, queue size: %s' % (pd_type, ct_dir, b_id, self.workEnv.queues[b_id].qsize()))

                    for item in self.read_offline_data(p_file, pd_type):
                        d_cnt += 1
                        if d_cnt % 1000 == 0:
                            print('read data:', d_cnt) #time.sleep(0.01)
                        all_data.append((b_id, item))
        p_cnt = 0
        for item in all_data:
            p_cnt += 1
            if p_cnt % 1000 == 0:
                print('produced data:', p_cnt)
            yield b_id, item

class TransDConsumer(px.Consumer):
    def __init__(self, consumer_id):
        self._consumer_id = consumer_id
        super(TransDConsumer, self).__init__(consumer_id)
        self._stg = stg.SimpleStrategy('option_stg_v1')

    def consume(self, item):
        #print('consumer_id: %s, 1 item produced' % (self._consumer_id))
        self._stg.step(item)


def init_data_task(root_path):
    ct_dirs = futil.get_sub_dirs(root_path)
    paths = []
    for ct_dir in ct_dirs:
        paths.append(os.path.abspath(os.path.join(root_path, ct_dir)))
    return paths

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    print('Called with args:')
    print(args)

    assert args.data_path

    input_data_paths = init_data_task(args.data_path)

    # create producer and consumers
    producers = []
    for producerId in range(_DEFAULT_PRODUCER_NUM):
        producerToStart = TransDProducer("producer.%d" % producerId, fp=args.data_path)
        producers.append(producerToStart)

    consumers = []
    for consumerId in range(sum(M_PD_BUCKET.values())):
        consumerToStart = TransDConsumer(consumerId)
        consumers.append(consumerToStart)

    worker = px.Worker(producers, consumers, 300000)
    worker.work()

    #print()
    #print('============EMA(N=12)==============') 
    #X = _ct_data['IC2206'].close
    #for day in range(len(X)):
    #    print("day {}: non-recursion: {}".format(day+1, op_util.ema(X[:day+1], 12)))

