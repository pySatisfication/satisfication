import redis
import json
from redis_helper import RedisHelper

#config = self.env.ref('sf_conference_management.wxagent_config_detail_conference_default').sudo()

obj = RedisHelper(host='localhost', port=6379, db=14, channel='tick:sc2206', password='')

message_dict = {
        "aa":"aaa", 
        "bb":"bbb",
        "aa1":"aaa", 
        "bb1":"bbb",
        "aa2":"aaa", 
        "bb2":"bbb",
        "aa3":"aaa", 
        "bb3":"bbb"
    }

for user_id, message in message_dict.items():
    info = {
            'description': message,
            'user_id': user_id,
            }
    print(f'写入信息为:{info}')
    obj.public(msg=json.dumps(info))
