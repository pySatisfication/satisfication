import sys
import time
import json
import logging
import logging.config
import argparse

from utils import redis_util

redis_handler = redis_util.RedisHandler()

config_file = '../conf/logger_config_pack.json'
with open(config_file, 'r', encoding='utf-8') as file:
    logging.config.dictConfig(json.load(file))
logger = logging.getLogger(__name__)

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='arguments for modular call')
    parser.add_argument('--depth_source', dest='depth_source', help='transaction data file',
                        default='../data/depth_data.csv', type=str)
    parser.add_argument('--debug', dest='debug', action="store_true", help='debug mode')
    #parser.add_argument('--target_file_name', dest='target_file_post', help='transaction data file',
    #                    default='', type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

def main_ct_check():
    # 1. 获取所有有效合约
    cts = redis_handler.get(redis_util.REDIS_KEY_VALID_CT)
    if not cts:
        logger.warning('no contract recorded in redis...')
        return

    # 2. 主次合约判断
    d_cp_cv, d_code_depth = {}, {}
    for ct_code in cts.split(','):
        j_d = json.loads(redis_handler.get(redis_util.REDIS_KEY_DEPTH_PREFIX + ct_code))
        d_code_depth[ct_code] = j_d

        code = j_d['symbol']
        code_prefix = j_d['code_prefix']
        volume = j_d['volume']

        if not code_prefix or code_prefix == '':
            continue
        if code_prefix in d_cp_cv:
            d_cp_cv[code_prefix][code] = volume
        else:
            d_cp_cv[code_prefix] = {code: volume}

    d_cp_msct = {}
    for k, v in d_cp_cv.items():
        sort_lst = sorted(v.items(), key=lambda x: x[1], reverse=True)
        idx = 0
        main_ct, sec_ct = '', ''
        for item in sort_lst:
            if idx == 0:
                main_ct = item[0]
            elif item[0] > main_ct:
                sec_ct = item[0]
                break
            idx += 1
        d_cp_msct[k] = main_ct + ',' + sec_ct
    redis_handler.set(redis_util.REDIS_KEY_MSCT, json.dumps(d_cp_msct))
    return d_cp_msct, d_code_depth

if __name__ == '__main__':
    # 解析参数
    #args = parse_args()
    #assert args.depth_source

    while True:
        time.sleep(0.5)

        # 1. 主次合约检测
        start_ms = time.time()
        d_cp_msct, d_code_depth = main_ct_check()
        end_ms = time.time()

        # 2. 打包生成首页合约列表
        page_ct_lst = {}
        # depth object array
        d_obj_arr = {}
        ct_idx = 1
        for k, v in d_cp_msct.items():
            in_idx = 0
            for ct_code in v.split(','):
                if ct_code in d_code_depth:
                    depth = d_code_depth[ct_code]
                    depth['order'] = ct_idx
                    if in_idx == 0:
                        depth['type'] = 'M'
                    else:
                        depth['type'] = 'SM'
                    d_obj_arr.append(depth)
                    ct_idx += 1
                in_idx += 1
        page_ct_lst['stock'] = d_obj_arr

        redis_handler.set(redis_util.REDIS_KEY_CT_LIST, json.dumps(page_ct_lst))
        logger.info('pack success, cost of main ct checking:{}...'.format(end_ms - start_ms))
