import redis
import logging
logger = logging.getLogger(__name__)
 
class RedisHelper(object):
    """
    host: redis ip
    port: redis port
    channel: 发送接送消息的频道
    """
    def __init__(self, host, port, db, channel, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.channel = channel
        self.__conn = redis.Redis(decode_responses=True)
 
    def ping(self):
        try:
            self.__conn.ping()
            return True
        except Exception as e:
            logger.exception(f"连接Redis失败，失败原因为：{e}")
            return False
 
    # 发送消息
    def public(self, msg):
        if self.ping():
            self.__conn.publish(self.channel, msg)
            return True
        else:
            return False
 
    # 订阅
    def subscribe(self):
        if self.ping():
            pub = self.__conn.pubsub()
            pub.subscribe(self.channel)
            pub.parse_response()
            return pub
        else:
            return False

