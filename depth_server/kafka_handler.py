import time
import logging

from depth import Depth
from kline import KLine
from kafka import KafkaConsumer,KafkaProducer,TopicPartition

KAFKA_SERVER = 'localhost:9092'
CONSUMER_GROUP_ID = 'k_depth_c1'
AUTO_OFFSET_RESET = 'latest'

FUTURES_DEPTH_TOPIC = 'FuturesDepthDataTest'
FUTURES_KLINE_TPOIC = 'FuturesKLineTest'

class KafkaHandler(object):
    def __init__(self, config_file=None):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])

        self.consumer = KafkaConsumer(auto_offset_reset=AUTO_OFFSET_RESET,
                                      group_id=CONSUMER_GROUP_ID,
                                      bootstrap_servers=[KAFKA_SERVER])
        self.consumer.assign([TopicPartition(FUTURES_DEPTH_TOPIC, self._hid)])

    def __iter__(self):
        return self

    def __next__(self):
        for msg_data in self.consumer:
            if msg_data is None or len(msg_data.value) == 0:
                continue
            cur_msg = msg_data.value.decode('utf-8').split(',')

            start = time.time()
            depth = Depth(cur_msg, start)
            yield depth

if __name__ == '__main__':
    kh = KafkaHandler()
    for item in kh:
        print(item)

