import time
import logging
import traceback

from depth import Depth
from kline import KLine
from kafka import KafkaConsumer,KafkaProducer,TopicPartition

KAFKA_SERVER = 'localhost:9092'
CONSUMER_GROUP_ID = 'k_depth_c1'
AUTO_OFFSET_RESET = 'latest'

FUTURES_DEPTH_TOPIC = 'FuturesDepthDataTest'
FUTURES_KLINE_TPOIC = 'FuturesKLineTest'

class KafkaHandler(object):
    def __init__(self, b_id, config_file=None, debug=True):
        self._bid = b_id
        self.logger = logging.getLogger(__name__)
        if debug:
            kzt = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            kzt.setFormatter(formatter)
            self.logger.addHandler(kzt)
        self.build_producer()
        self.build_consumer()

    def build_producer(self):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])
        if self.producer.bootstrap_connected():
            self.logger.info('[KafkaHandler]build kafka producer successful')

    def build_consumer(self):
        # consumer
        self.consumer = KafkaConsumer(auto_offset_reset=AUTO_OFFSET_RESET,
                                      group_id=CONSUMER_GROUP_ID,
                                      bootstrap_servers=[KAFKA_SERVER])
        self.consumer.assign([TopicPartition(FUTURES_DEPTH_TOPIC, self._bid)])

        if self.consumer.bootstrap_connected():
            self.logger.info('[KafkaHandler]build kafka consumer successful')
            for tp in self.consumer.assignment():
                self.logger.info('[KafkaHandler]topic:{}, partition:{}'.format(tp[0], tp[1]))

    def produce(self, k_item):
        if not self.producer.bootstrap_connected():
            retry_times = 3
            while retry_times > 0:
                try:
                    self.logger.info('[KafkaHandler]connect with kafka server, rest times: {}'.format(
                        retry_times))
                    self.build_producer()
                except Exception as e:
                    self.logger.error('[KafkaHandler]error:{}, rest times: {}'.format(e, retry_times))
                if self.producer.bootstrap_connected():
                    break
                retry_times -= 1
            if not self.producer.bootstrap_connected():
                self.logger.error('[KafkaHandler]Unable to connect to the server. prepare to exit.')
                return
        # 正常连接
        future = self.producer.send(FUTURES_KLINE_TPOIC,
                                    str(k_item).encode('utf-8'),
                                    partition=self._bid)
        try:
            future.get(timeout=5)
        except Exception as e:
            self.logger.error('[gen_kline]send kline message error:', e)
            traceback.format_exc()

    def consume(self):
        if not self.consumer.bootstrap_connected():
            retry_times = 3
            while retry_times > 0:
                try:
                    self.logger.info('[KafkaHandler]connect with kafka server, rest times: {}'.format(
                        retry_times))
                    self.build_consumer()
                except Exception as e:
                    self.logger.error('[KafkaHandler]error:{}, rest times: {}'.format(e, retry_times))
                if self.consumer.bootstrap_connected():
                    break
                retry_times -= 1
            if not self.consumer.bootstrap_connected():
                self.logger.error('[KafkaHandler]Unable to connect to the server. prepare to exit.')
                return
        # 正常连接
        for msg_data in self.consumer:
            if msg_data is None or len(msg_data.value) == 0:
                continue
            cur_msg = msg_data.value.decode('utf-8').split(',')

            start = time.time()
            depth = Depth(cur_msg, start)
            yield depth

if __name__ == '__main__':
    kh = KafkaHandler(0)
    for item in kh.consume():
        print(item)

