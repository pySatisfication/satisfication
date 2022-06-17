import sys
import threading
import argparse

from kline_handler import *
from tran_time_helper import *

HACK_DELAY = 0.02

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='arguments for modular call')
    parser.add_argument('--depth_source', dest='depth_source', help='transaction data file',
                        default='../data/depth_data.csv', type=str)
    #parser.add_argument('--target_file_name', dest='target_file_post', help='transaction data file',
    #                    default='', type=str)

    parser.add_argument('--scan_delay_time', dest='scan_delay_time', help='interval time to generate k for last cache of each period',
                        default=59, type=int)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    #print('program starting...')

    # 并发处理
    depth_event = threading.Event()
    consumers = []
    for consumerId in range(NUM_HANDLER):
        consumer = KHandlerThread(consumerId, depth_event)
        consumers.append(consumer)
        consumer.start()
    for consumer in consumers:
        consumer.join(HACK_DELAY)
        
    depth_event.set()
    # 记录分桶
    m_ccode_id = {}
    # 时间处理
    tth = TranTimeHelper()

    args = parse_args()
    #print(args)
    assert args.depth_source
    #assert args.target_file_post

    next_time_span = []

    if args.depth_source == 'kafka':
        consumer = KafkaConsumer('FuturesDepthData', auto_offset_reset='earliest', bootstrap_servers= ['localhost:9092'])
        for msg_data in consumer:
            assert len(msg_data) != 0
            cur_msg = msg_data.split(',')
            code = cur_msg[1]

            # DEBUG
            if cur_msg[0] < '20220530':
                continue

            # 非交易时段(成交量)
            if cur_msg[3] == '0':
                continue
            if tth.check_in(1, code, msg_data[10]):
                continue

            depth = Depth(cur_msg)

            # 计算分桶
            if code in m_ccode_id:
                b_offset = m_ccode_id[code]
            else:
                b_offset = len(m_ccode_id)
                m_ccode_id[code] = b_offset

            b_id = b_offset % NUM_HANDLER
            # 广播消息
            queues[b_id].put(depth, True, HACK_DELAY)
    else:
        with open(args.depth_source, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                assert len(line) != 0

                cur_msg = line.split(',')
                code = cur_msg[1]

                #print(cur_msg)
                #continue

                # DEBUG
                #if cur_msg[0] > '20220530':
                #    continue

                depth = Depth(cur_msg)

                # 异常数据过滤
                isin, once_out = False, False
                if len(next_time_span) > 0:
                    for item in next_time_span:
                        span = item.split(',')
                        if (depth.update_time[0:2] < span[1] and depth.update_time[0:2] >= span[0]) \
                            or (depth.update_time[0:2] <= span[1] and depth.update_time[0:2] > span[0]):
                            isin = True
                            once_out = False
                        elif depth.update_time[0:2] == span[1] and depth.update_time[0:2] == span[0]:
                            if not once_out:
                                isin = True
                        else:
                            once_out = True
                    if not isin:
                        continue
                if depth.update_time[0:2] == '02':
                    next_time_span = ['02,02', '08,11']
                elif depth.update_time[0:2] == '11':
                    next_time_span = ['11,11', '13,15']
                elif depth.update_time[0:2] == '15:00:00':
                    next_time_span = ['15,15', '20,23']
                elif depth.update_time[0:2] == '23':
                    next_time_span = ['23,23', '00,02']

                #if depth.update_time == '13:30:00':
                #    print('depth time=13:30:00, main thread wait 180s..')
                #    time.sleep(180)

                # 非交易时段(成交量)
                if depth.volume == 0.0:
                    continue
                if not tth.check_in(0, code, depth.update_time) and not tth.check_in(2, code, depth.update_time):
                    continue

                # 计算分桶
                if code in m_ccode_id:
                    b_offset = m_ccode_id[code]
                else:
                    b_offset = len(m_ccode_id)
                    m_ccode_id[code] = b_offset

                b_id = b_offset % NUM_HANDLER
                # 广播消息
                queues[b_id].put(depth, True, HACK_DELAY)
