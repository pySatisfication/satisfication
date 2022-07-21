import sys
import time
import logging
import traceback

sys.path.append("..")
from depth_server.depth import Depth
from depth_server.kline import KLine
from kafka import KafkaConsumer,KafkaProducer,TopicPartition

KAFKA_SERVER = 'localhost:9092'
AUTO_COMMIT_INTERVAL_MS = 1000

class KafkaHandler(object):
    def __init__(self,
                 b_id,
                 producer_topic,
                 consumer_group_id,
                 consumer_auto_offset_reset,
                 consumer_topic,
                 config_file=None,
                 debug=False):
        self._bid = b_id
        self.logger = logging.getLogger(__name__)
        if debug:
            kzt = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            kzt.setFormatter(formatter)
            self.logger.addHandler(kzt)

        self._producer_topic = producer_topic
        self._group_id = consumer_group_id
        self._auto_offset_reset = consumer_auto_offset_reset
        self._consumer_topic = consumer_topic
        if consumer_topic:
            self.build_consumer()
        if producer_topic:
            self.build_producer()

    def build_producer(self):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])
        if self.producer.bootstrap_connected():
            self.logger.info('[KafkaHandler]build kafka producer successful')

    def build_consumer(self):
        # consumer
        self.consumer = KafkaConsumer(auto_offset_reset=self._auto_offset_reset,
                                      group_id=self._group_id,
                                      bootstrap_servers=[KAFKA_SERVER],
                                      auto_commit_interval_ms=AUTO_COMMIT_INTERVAL_MS)
        self.consumer.assign([TopicPartition(self._consumer_topic, self._bid)])

        if self.consumer.bootstrap_connected():
            #print('build consumer success, handler:%s' % self._bid)
            self.logger.info('[KafkaHandler]build kafka consumer successful')
            for tp in self.consumer.assignment():
                #print('[KafkaHandler]handler:{}, topic:{}, partition:{}'.format(self._bid, tp[0], tp[1]))
                self.logger.info('[KafkaHandler]handler:{}, topic:{}, partition:{}'.format(self._bid, tp[0], tp[1]))
        else:
            #print('build consumer failed, handler:%s' % self._bid)
            pass

    def produce(self, k_item):
        #if not self.producer.bootstrap_connected():
        #    retry_times = 3
        #    while retry_times > 0:
        #        try:
        #            self.logger.info('[KafkaHandler]connect with kafka server, rest times: {}'.format(
        #                retry_times))
        #            self.build_producer()
        #        except Exception as e:
        #            self.logger.error('[KafkaHandler]error:{}, rest times: {}'.format(e, retry_times))
        #        if self.producer.bootstrap_connected():
        #            break
        #        retry_times -= 1
        #    if not self.producer.bootstrap_connected():
        #        self.logger.error('[KafkaHandler]Unable to connect to the server. prepare to exit.')
        #        return
        # 正常连接
        future = self.producer.send(self._producer_topic,
                                    str(k_item).encode('utf-8'),
                                    partition=self._bid)
        try:
            future.get(timeout=5)
        except Exception as e:
            self.logger.error('[gen_kline]send kline message error:', e)
            traceback.format_exc()

    def consume(self):
        #if not self.consumer.bootstrap_connected():
        #    retry_times = 3
        #    while retry_times > 0:
        #        try:
        #            self.logger.info('[KafkaHandler]connect with kafka server, rest times: {}'.format(
        #                retry_times))
        #            self.build_consumer()
        #        except Exception as e:
        #            self.logger.error('[KafkaHandler]error:{}, rest times: {}'.format(e, retry_times))
        #        if self.consumer.bootstrap_connected():
        #            break
        #        retry_times -= 1
        #    if not self.consumer.bootstrap_connected():
        #        self.logger.error('[KafkaHandler]Unable to connect to the server. prepare to exit.')
        #        return
        # 正常连接
        for msg_data in self.consumer:
            if msg_data is None or len(msg_data.value) == 0:
                continue
            yield msg_data

if __name__ == '__main__':
    kh = KafkaHandler(0)
    # test consumer
    #for item in kh.consume():
    #    print(item)

    # test producer
    kline = KLine(['test_code','period','20220601 10:00:00',0.0,0.0,0.0,0.0,0.0,0.0,0.0])
    print('wait...')
    time.sleep(20)

    #print('close')
    #kh.producer.close()
    print('send message...')
    kh.produce(kline)

