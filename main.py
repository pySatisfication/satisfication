# coding=utf-8
import os
import sys
import time
import json
import threading
import argparse
import traceback
import logging
import logging.config
from easydict import EasyDict as edict
from kafka import KafkaConsumer,KafkaProducer

if sys.version > '3':
    import queue as Queue
else:
    import Queue

sys.path.append("..")
from utils import dt_util,db_util,kafka_util
from depth_server.kline import KLine

import strategy as stg
import parallel as px
import utils.util as op_util
import utils.file_util as futil

_PRODUCTION_DELAY = 0.05
_CONSUMPTION_DELAY = 0.05
_DEFAULT_PRODUCER_NUM = 1
_DEFAULT_CONSUMER_NUM = 3

MQ_KAFKA = 'kafka'

HACK_DELAY = 0.2
NUM_KLINE_HANDLER = 6
LOCAL_QUEUE_SIZE = 1000000

# logger
index_config_file = 'conf/logger_config_index.json'
with open(index_config_file, 'r', encoding='utf-8') as file:
    logging.config.dictConfig(json.load(file))
logger = logging.getLogger("kindex_service")

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

CONSUMER_GROUP_ID = 'index_kline_g1'
AUTO_OFFSET_RESET = 'latest'
FUTURES_KLINE_TPOIC = 'FuturesKLineTest'

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='arguments for modular call')
    parser.add_argument('--data_source', dest='data_source', help='transaction data source',
                        default='trans_data.txt', type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

class TransDProducer(px.Producer):
    def __init__(self, name, source, itemProducerFailsAt=None):
        assert name
        super(TransDProducer, self).__init__(name)
        self._data_source = source
        self._m_code_id = {}

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
                               period = pd_type)
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
        if self._data_source == 'kafka':
            print('k source is kafka...')
            consumer = KafkaConsumer(FUTURES_DEPTH_TOPIC, auto_offset_reset='latest',
                                     bootstrap_servers=['localhost:9092'])
            for msg_data in consumer:
                assert msg_data is not None
                if msg_data is None or len(msg_data.value) == 0:
                    continue
                depth = msg_data.value.decode('utf-8')
                b_id = depth.instrument_id
                yield b_id, item
        else:
            with open(args.depth_source, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    assert len(line) != 0

                    depth_data_iterate(line, time.time())
            print('File read ended!!!')
            self.log.info("producing items...")

            # init start bucketID for each period data
            self.init_start_bid()

            ct_dirs = futil.get_sub_dirs(self._data_source)
            all_data = []
            for ct_dir in ct_dirs:
                if ct_dir != 'LH2207':
                    continue
                p_dirs = futil.get_sub_dirs(os.path.join(self._data_source, ct_dir))

                for p_dir in p_dirs:
                    if p_dir not in ('3', '4'):
                        continue
                    # 4:day, 5:hour, 6:minute, 7:second
                    p_files = sorted(futil.get_all_files(os.path.join(self._data_source, ct_dir, p_dir), 'spt'))
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

                        if ct_dir in self._m_code_id:
                            b_offset = self._m_code_id[ct_dir]
                        else:
                            b_offset = len(self._m_code_id)
                            self._m_code_id[ct_dir] = b_offset

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

SUPPORT_STG_NAMES = ['simple']
class TransDConsumer(threading.Thread):
    def __init__(self,
                 b_id,
                 t_control_event,
                 data_source,
                 local_queue,
                 stg_name = 'simple',
                 config_file='conf/index_logger_config.json'):
        threading.Thread.__init__(self)
        self._bid = b_id
        self._t_control_event = t_control_event
        self._data_source = data_source

        # 初始化策略
        self.init_stgs()
        self._valid_stg = self._stgs[stg_name]

        if self._data_source == MQ_KAFKA:
            # kafka处理器，无生产者
            self.kafka_handler = kafka_util.KafkaHandler(
                b_id=self._hid,
                producer_topic=None,
                consumer_group_id=CONSUMER_GROUP_ID,
                consumer_auto_offset_reset=AUTO_OFFSET_RESET,
                consumer_topic=FUTURES_KLINE_TPOIC,
                config_file=config_file)
        else:
            self._local_queue = local_queue

    def init_stgs(self):
        self._stgs = {}
        for stg_name in SUPPORT_STG_NAMES:
            if stg_name == 'simple':
                self._stgs[stg_name] = stg.SimpleStrategy('option_stg_v1')

    def run(self):
        try:
            print('consumer start to run...:', self._bid)
            self._t_control_event.wait()

            cnt_id = 0
            while self._t_control_event.isSet():
                if self._data_source == MQ_KAFKA:
                    for msg_data in self.kafka_handler.consume():
                        k_data = msg_data.value.decode('utf-8').split(',')
                        if len(k_data) < 10:
                            logger.error('[run]data length error: {}'.format(k_data))
                            continue
                        kline = KLine(k_data, time.time())
                        self.consume(kline)

                        cnt_id += 1
                        if cnt_id > 1000000:
                            cnt_id -= 1000000
                        if cnt_id % 100 == 0:
                            logger.info('[run]consumer: {}, processed: {}'.format(self._bid, cnt_id))
                else:
                    while True:
                        # 阻塞模式，不用捕捉空异常
                        kline = self._local_queue.get()

                        # depth计算
                        self.consume(kline)

                        # 计时
                        end = time.time()
                        cnt_id += 1
                        if cnt_id % 1000 == 0:
                            self.logger.info("[run]code:{}, bucket_id:{}, cost of depth:{}".format(
                                kline.code, self._hid, end - kline.sys_time))
                        if cnt_id > 10000000:
                            cnt_id = 0
        except Exception as error:
            logger.warning("cannot continue to consume: %s", error)
            exc_info = sys.exc_info()
            logger.error("raising notified error: %s %s", exc_info[0], exc_info[1])
            for filename, linenum, funcname, source in traceback.extract_tb(exc_info[2]):
                logger.warning("%-23s:%s '%s' in %s", filename, linenum, source, funcname)

    def consume(self, item):
        j_idx_str = self._valid_stg.step(item)
        #logger.info("[consume]index step: {}".format(j_idx_str))

def init_data_task(root_path):
    ct_dirs = futil.get_sub_dirs(root_path)
    paths = []
    for ct_dir in ct_dirs:
        paths.append(os.path.abspath(os.path.join(root_path, ct_dir)))
    return paths

def kline_step(data, sys_time=None):
    cur_msg = data.split(',')
    kline = KLine(cur_msg, sys_time)
    code = kline.code

    # 计算分桶
    if code in m_code_id:
        b_offset = m_code_id[code]
    else:
        b_offset = len(m_code_id)
        m_code_id[code] = b_offset
        logger.info("[kline_step]code:{}, bucket:{}".format(code, b_offset))

    b_id = b_offset % NUM_KLINE_HANDLER
    # 广播消息
    local_queues[b_id].put(kline, True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    print('Called with args:')
    print(args)
    assert args.data_source

    m_code_id = {}

    #input_data_paths = init_data_task(args.data_source)

    # k线数据源
    if args.data_source == MQ_KAFKA:
        data_source = MQ_KAFKA
        local_queues = [None for i in range(NUM_KLINE_HANDLER)]
    else:
        data_source = args.depth_source.split('/')[-1].split('.')[0]
        local_queues = [Queue.Queue(LOCAL_QUEUE_SIZE) for i in range(NUM_KLINE_HANDLER)]

    kindex_event = threading.Event()
    consumers = []
    for consumer_id in range(NUM_KLINE_HANDLER):
        consumer = TransDConsumer(consumer_id,
                                  kindex_event,
                                  data_source,
                                  local_queues[consumer_id])
        consumers.append(consumer)
        consumer.start()
    for consumer in consumers:
        consumer.join(HACK_DELAY)
    kindex_event.set()

    if args.depth_source == MQ_KAFKA:
        pass
    else:
        with open(args.data_source, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                assert len(line) != 0

                kline_step(line, time.time())
        print('File read ended!!!')

    ## create producer and consumers
    #producers = []
    #for producerId in range(_DEFAULT_PRODUCER_NUM):
    #    producerToStart = TransDProducer("producer.%d" % producerId, source=args.data_source)
    #    producers.append(producerToStart)

    #consumers = []
    #for consumerId in range(sum(M_PD_BUCKET.values())):
    #    consumerToStart = TransDConsumer(consumerId)
    #    consumers.append(consumerToStart)

    #worker = px.Worker(producers, consumers, 300000)
    #worker.work()

    #print()
    #print('============EMA(N=12)==============') 
    #X = _ct_data['IC2206'].close
    #for day in range(len(X)):
    #    print("day {}: non-recursion: {}".format(day+1, op_util.ema(X[:day+1], 12)))

