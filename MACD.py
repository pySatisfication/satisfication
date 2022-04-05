import math

import EMA

import pandas as pd
from matplotlib import pyplot as plt

def print_min():
    # Use a breakpoint in the code line below to debug your script.
    m = 123
    n = 150
    print(f"m={m} n={n}")
    print(f"Min:", min(m, n))


def print_MACD():
    X_N = [3, -4, 5, 6, 7]
    X = [3, 4, 5, 6, 7]

    N = 5
    print(EMA.EMA(X, N))



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    X = [3, 6, 8, 4, 9, 12, 16, 8, 10]

    r = [
        {
            'Time': '2022-01-01',
            'Close': 30,
            'Open': 0,
            'High': 0,
            'Low': 0,
            'Volume': 0,
        },
        {
            'Time': '2022-01-02',
            'Close': 25.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },        {
            'Time': '2022-01-03',
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-04',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-05',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-06',
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        }, {
            'Time': '2022-01-07',
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-08',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-09',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-10',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-11',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-12',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-13',
            'Close': 3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-14',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-15',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-16',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-17',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-18',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-19',
            'Close': 3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-20',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-21',
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        }, {
            'Time': '2022-01-22',
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-23',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-24',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-25',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-26',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-27',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-28',
            'Close': 3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-29',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-30',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': '2022-01-31',
            'Close': 15,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        # {
        #     'Time': '2022-02-01',
        #     'Close': 9,
        #     'Open': 6.5,
        #     'High': 10,
        #     'Low': 4.5,
        #     'Volume': 100,
        # },
        # {
        #     'Time': '2022-02-02',
        #     'Close': 5,
        #     'Open': 6.5,
        #     'High': 10,
        #     'Low': 4.5,
        #     'Volume': 100,
        # },
        # {
        #     'Time': '2022-02-03',
        #     'Close': 3,
        #     'Open': 6.5,
        #     'High': 10,
        #     'Low': 4.5,
        #     'Volume': 100,
        # },

    ]


    quick_period = 4
    slow_period = 2

    # quick_line_node = EMA.EMA(X, quick_period) - EMA.EMA(X, slowly_period)

    quick_line = EMA.EMA(r, quick_period, 'Close')
    print("quick:", quick_line)

    slow_line = EMA.EMA(r, slow_period, 'Close')
    print("slow:", slow_line)

    len_q = len(quick_line)

    # 顶背离

    macd_ret = []
    for i in range(-1, len_q-1):
        b_macd_ret = 0
        if (i > 1) and (quick_line[i-1] > slow_line[i-1]) and (quick_line[i] >= slow_line[i]) and (quick_line[i+1] < slow_line[i+1]):
            # 死叉
            b_macd_ret = -1

        if (i > 1) and (quick_line[i-1] < slow_line[i-1]) and (quick_line[i] <= slow_line[i]) and (quick_line[i+1] > slow_line[i+1]):
            # 金叉
            b_macd_ret = 1
        macd_ret.append(b_macd_ret)

    print("macd_ret:", macd_ret)


# _dates = ['2022-03-22 16:45:08', '2022-03-22 16:46:08', '2022-03-22 16:47:08', '2022-03-22 16:48:08',
#           '2022-03-22 16:49:08', '2022-03-22 16:50:08', '2022-03-22 16:51:08', '2022-03-22 16:52:08',
#           '2022-03-22 16:53:08', '2022-03-22 16:54:08']


    _data_close = [vc['Close'] for vc in r]
    _dates = [v['Time'] for v in r]

    _data1 = quick_line
    _data2 = slow_line

    di = pd.DatetimeIndex(_dates,
                          dtype='datetime64[ns]', freq=None)

    pd.DataFrame({'data1': _data1},
                 index=di).plot.line()  # 图形横坐标默认为数据索引index。
    #
    plt.savefig(r'data/p1.png', dpi=200)
    plt.show()  # 显示当前正在编译的图像

    pd.DataFrame({'data1': _data1, 'data2': _data2},
                 index=di).plot.line()  # 图形横坐标默认为数据索引index。
    #
    plt.savefig(r'data/p2.png', dpi=200)
    plt.show()  # 显示当前正在编译的图像

    MACD_INDEX_T0 = 0
    for j in range(len_q-1, 0):
        current = r[j]
        val = macd_ret[j]
        # 死叉
        if val == -1:
            MACD_INDEX_T0 = j
            break

    MACD_INDEX_T1 = 0
    for k in range(len_q-1, 0):
        current = r[k]
        val = macd_ret[k]
        # 金叉
        if val == 1:
            MACD_INDEX_T1 = k
            break

            # MACD_MAX1 = current['Close']
            # MACD_MAX2 = current['Close']
    if MACD_INDEX_T0 > MACD_INDEX_T1:
        # 最后一个是死叉  判断顶背离
        b_down = -1
    else:
        b_down = 1

    DOWN_MACD_MAX1 = 0
    if b_down == -1:
        DOWN_MACD_MAX1 = max(_data_close[MACD_INDEX_T1:MACD_INDEX_T0])
        print("DOWN_MACD_MAX1:", DOWN_MACD_MAX1)

    MACD_T0_INDEX2 = 0
    for j2 in range(MACD_INDEX_T1-1, 0):
        current = r[j2]
        val = macd_ret[j2]
        # 死叉
        if val == -1:
            MACD_T0_INDEX2 = j2
            break

    MACD_T1_INDEX2 = 0
    for k2 in range(MACD_INDEX_T1-1, 0):
        current = r[k2]
        val = macd_ret[k2]
        # 金叉
        if val == 1:
            MACD_T1_INDEX2 = k2
            break

    DOWN_MACD_MAX2 = 0
    if b_down == -1:
        DOWN_MACD_MAX2 = max(_data_close[MACD_T1_INDEX2:MACD_T0_INDEX2])
        print("DOWN_MACD_MAX2:", DOWN_MACD_MAX2)

    if DOWN_MACD_MAX1 >= DOWN_MACD_MAX2:
        print("MACD 顶背离！！！")
