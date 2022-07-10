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

        # producer
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])

        # consumer
        self.consumer = KafkaConsumer(auto_offset_reset=AUTO_OFFSET_RESET,
                                      group_id=CONSUMER_GROUP_ID,
                                      bootstrap_servers=[KAFKA_SERVER])
        self.consumer.assign([TopicPartition(FUTURES_DEPTH_TOPIC, self._bid)])

        if self.consumer.bootstrap_connected():
            self.logger.info('[KafkaHandler]kafka connection success.')
            for tp in self.consumer.assignment():
                self.logger.info('[KafkaHandler]topic:{}, partition:{}'.format(tp[0], tp[1]))

    def produce(self, k_item):
        future = self.producer.send(FUTURES_KLINE_TPOIC,
                                    k_item.print_line().encode('utf-8'),
                                    partition=self._bid)
        try:
            future.get(timeout=5)
        except Exception as e:
            self.logger.error('[gen_kline]send kline message error:', e)
            traceback.format_exc()

    def items(self):
        for msg_data in self.consumer:
            if msg_data is None or len(msg_data.value) == 0:
                continue
            cur_msg = msg_data.value.decode('utf-8').split(',')

            start = time.time()
            depth = Depth(cur_msg, start)
            yield depth

if __name__ == '__main__':
    kh = KafkaHandler(0)
    for item in kh.items():
        print(item)

