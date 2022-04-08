import math

import EMA

import pandas as pd
from matplotlib import pyplot as plt
import numpy as np


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
            'Volume': 50,
        },
        {
            'Time': '2022-01-02',
            'Close': 25.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 60,
        },        {
            'Time': '2022-01-03',
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 65,
        },
        {
            'Time': '2022-01-04',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 70,
        },
        {
            'Time': '2022-01-05',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 75,
        },
        {
            'Time': '2022-01-06',
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 80,
        }, {
            'Time': '2022-01-07',
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 65,
        },
        {
            'Time': '2022-01-08',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 90,
        },
        {
            'Time': '2022-01-09',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 30,
        },
        {
            'Time': '2022-01-10',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 40,
        },
        {
            'Time': '2022-01-11',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 45,
        },
        {
            'Time': '2022-01-12',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 55,
        },
        {
            'Time': '2022-01-13',
            'Close': 3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 60,
        },
        {
            'Time': '2022-01-14',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 50,
        },
        {
            'Time': '2022-01-15',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 70,
        },
        {
            'Time': '2022-01-16',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 80,
        },
        {
            'Time': '2022-01-17',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 88,
        },
        {
            'Time': '2022-01-18',
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 80,
        },
        {
            'Time': '2022-01-19',
            'Close': 4.7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 77,
        },
        {
            'Time': '2022-01-20',
            'Close': 6.6,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 50,
        },
        {
            'Time': '2022-01-21',
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 66,
        }, {
            'Time': '2022-01-22',
            'Close': 4.9,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 80,
        },
        {
            'Time': '2022-01-23',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 70,
        },
        {
            'Time': '2022-01-24',
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 60,
        },
        {
            'Time': '2022-01-25',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 75,
        },
        {
            'Time': '2022-01-26',
            'Close': 7,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 89,
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
            'Close': 4.3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 90,
        },
        {
            'Time': '2022-01-29',
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 80,
        },
        {
            'Time': '2022-01-30',
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 50,
        },
        {
            'Time': '2022-01-31',
            'Close': 15,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 30,
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


    C_period1 = 3 #12
    C_period2 = 6 #26
    DIFF_period = 5 #9

    # quick_line_node = EMA.EMA(X, quick_period) - EMA.EMA(X, slowly_period)

    C_period1_line = EMA.EMA(r, C_period1, 'Close')
    C_period2_line = EMA.EMA(r, C_period2, 'Close')
    len_q = len(r)

    print("len_q:", len_q)

    quick_point_line = []  # DIFF
    quick_line = []
    DIFF_line = []
    for i in range(0, len_q):
        point = r[i].copy()
        DIFF = round(C_period1_line[i] - C_period2_line[i], 2)
        point["Close"] = DIFF
        point_val = DIFF
        quick_point_line.append(point)
        quick_line.append(point_val)

    print("quick LINE POINT:", quick_point_line)
    print("len:", len(quick_point_line))
    print("quick LINE:", quick_line)
    print("len:", len(quick_line))

    C_slow_line = EMA.EMA(quick_point_line, DIFF_period, 'Close')

    slow_line = []  # DIFF
    for i in range(len_q):
        point_val = C_slow_line[i]
        slow_line.append(point_val)

    print("slow LINE:", slow_line)



    # 顶背离
    macd_ret = []
    for i in range(-1, len_q-1):
        b_macd_ret = 0
        if (i > 1) and (quick_line[i] > slow_line[i]) and (quick_line[i+1] < slow_line[i+1]):
            # 死叉
            b_macd_ret = -1
        if (i > 1) and (quick_line[i-1] > slow_line[i-1]) and (quick_line[i] >= slow_line[i]) and (quick_line[i+1] < slow_line[i+1]):
            # 死叉
            b_macd_ret = -1

        if (i > 1) and (quick_line[i-1] < slow_line[i-1]) and (quick_line[i] <= slow_line[i]) and (quick_line[i+1] > slow_line[i+1]):
            # 金叉
            b_macd_ret = 1
        if (i > 1) and (quick_line[i] < slow_line[i]) and (quick_line[i+1] > slow_line[i+1]):
            # 金叉
            b_macd_ret = 1

        macd_ret.append(b_macd_ret)

    print("macd_ret:", macd_ret)


# _dates = ['2022-03-22 16:45:08', '2022-03-22 16:46:08', '2022-03-22 16:47:08', '2022-03-22 16:48:08',
#           '2022-03-22 16:49:08', '2022-03-22 16:50:08', '2022-03-22 16:51:08', '2022-03-22 16:52:08',
#           '2022-03-22 16:53:08', '2022-03-22 16:54:08']


    # 收盘价
    _data_close = [vc['Close'] for vc in r]
    _dates = [v['Time'] for v in r]

    _data_volume = [vc['Volume'] for vc in r]

    _data1 = quick_line
    _data2 = slow_line

    di = pd.DatetimeIndex(_dates,
                          dtype='datetime64[ns]', freq=None)

    # pd.DataFrame({'data1': _data1},
    #              index=di).plot.line()  # 图形横坐标默认为数据索引index。
    # #
    # plt.savefig(r'data/p1.png', dpi=200)
    # plt.show()  # 显示当前正在编译的图像

    pd.DataFrame({'data1': _data1, 'data2': _data2},
                 index=di).plot.line()  # 图形横坐标默认为数据索引index。
    #
    plt.savefig(r'data/p2.png', dpi=200)
    plt.show()  # 显示当前正在编译的图像

    DOWN_MACD_INDEX_T0 = 0
    DOWN_MACD_INDEX_T2 = 0
    b_first_point = 0
    for j in range(len_q):
        print("J:", j)
        idx = len_q - j - 1
        print("idx:", idx)
        point_val = macd_ret[idx]
        # 死叉
        # 第一个死叉点位 T0
        if point_val == -1 and b_first_point == 0:
            DOWN_MACD_INDEX_T0 = idx
            b_first_point = 1
            print("DOWN_MACD_INDEX_T0:", DOWN_MACD_INDEX_T0)
            continue
        # 第二个死叉点位 T2
        elif point_val == -1 and b_first_point == 1:
            DOWN_MACD_INDEX_T2 = idx
            print("DOWN_MACD_INDEX_T2:", DOWN_MACD_INDEX_T2)
            break

    UP_MACD_INDEX_T1 = 0
    UP_MACD_INDEX_T3 = 0
    b_up_first_point = 0
    for k in range(len_q):
        print("K:", k)
        idx = len_q - k - 1
        print("idx:", idx)
        up_point_val = macd_ret[idx]
        # 金叉
        # 第一个金叉点位 T0
        if up_point_val == 1 and b_up_first_point == 0:
            UP_MACD_INDEX_T1 = idx
            print("T1:", UP_MACD_INDEX_T1)
            b_up_first_point = 1
            continue
        # 第二个金叉点位 T2
        elif up_point_val == 1 and b_up_first_point == 1:
            UP_MACD_INDEX_T3 = idx
            print("T3:", UP_MACD_INDEX_T3)
            break

            # MACD_MAX1 = current['Close']
            # MACD_MAX2 = current['Close']

    print("T0:", DOWN_MACD_INDEX_T0, "T2:", DOWN_MACD_INDEX_T2)
    print("T1:", UP_MACD_INDEX_T1,   "T3:", UP_MACD_INDEX_T3)

    if DOWN_MACD_INDEX_T0 > UP_MACD_INDEX_T1:
        # 最后一个是死叉  判断顶背离
        last_point = -1
        print("最后一个点为死叉！！！")
    else:
        # 金叉
        last_point = 1
        print("最后一个点为金叉！！！")


    # 最后死叉
    if last_point == -1:
        C_MAX1 = 0
        C_MAX1 = max(_data_close[UP_MACD_INDEX_T1:DOWN_MACD_INDEX_T0])
        print("C_MAX1:", C_MAX1)

        C_MAX2 = 0
        C_MAX2 = max(_data_close[UP_MACD_INDEX_T3:DOWN_MACD_INDEX_T2])
        print("C_MAX2:", C_MAX2)

        if C_MAX1 >= C_MAX2:
            print("当前状态为死叉， MACD 顶背离！！！")

            ################## 【快线顶背离】##################
            DIFF_MAX1 = 0
            DIFF_MAX1 = max(quick_line[UP_MACD_INDEX_T1:DOWN_MACD_INDEX_T0])
            print("DIFF_MAX1:", DIFF_MAX1)

            DIFF_MAX2 = 0
            DIFF_MAX2 = max(quick_line[UP_MACD_INDEX_T3:DOWN_MACD_INDEX_T2])
            print("DIFF_MAX2:", DIFF_MAX2)

            if DIFF_MAX1 < DIFF_MAX2:
                print("当前状态为死叉， 【快线顶背离】！！！")
            else:
                print("当前状态为死叉， 但没有出现 【快线顶背离】！！！")

            ################## 【MACD柱顶背离】##################
            MACD_MAX1 = 0
            MACD_MAX1 = max(_data_volume[UP_MACD_INDEX_T1:DOWN_MACD_INDEX_T0])
            print("MACD_MAX1:", MACD_MAX1)

            MACD_MAX2 = 0
            MACD_MAX2 = max(_data_volume[UP_MACD_INDEX_T3:DOWN_MACD_INDEX_T2])
            print("MACD_MAX2:", MACD_MAX2)

            if MACD_MAX1 < MACD_MAX2:
                print("当前状态为死叉， 【MACD柱顶背离】！！！")
            else:
                print("当前状态为死叉， 但没有出现 【MACD柱顶背离】！！！")


            ################## 【MACD柱面积顶背离】##################
            MACD_AREA_MAX1 = 0
            MACD_AREA_MAX1 = np.sum(_data_volume[UP_MACD_INDEX_T1:DOWN_MACD_INDEX_T0])
            print("MACD_AREA_MAX1:", MACD_AREA_MAX1)

            MACD_AREA_MAX2 = 0
            MACD_AREA_MAX2 = np.sum(_data_volume[UP_MACD_INDEX_T3:DOWN_MACD_INDEX_T2])
            print("MACD_AREA_MAX2:", MACD_AREA_MAX2)

            if MACD_AREA_MAX1 < MACD_AREA_MAX2:
                print("当前状态为死叉， 【MACD柱面积顶背离】！！！")
            else:
                print("当前状态为死叉， 但没有出现 【MACD柱面积顶背离】！！！")


        else:
            print("当前状态为死叉， 但是 MACD 不是顶背离！！！")

    # 最后金叉
    if last_point == 1:
        C_MIN1 = 0
        C_MIN1 = min(_data_close[DOWN_MACD_INDEX_T0:UP_MACD_INDEX_T1])
        print("C_MIN1:", C_MIN1)

        C_MIN2 = 0
        C_MIN2 = min(_data_close[DOWN_MACD_INDEX_T2:UP_MACD_INDEX_T3])
        print("C_MIN2:", C_MIN2)

        if C_MIN1 <= C_MIN2:
            print("当前状态为金叉， 开始判断 MACD 3种 底背离！！！")
            UP_NAME = "[金叉]"
            UP_DIFF_NAME = "[快线底背离]"

            ################## 【快线底背离】##################
            DIFF_MIN1 = 0
            DIFF_MIN1 = min(quick_line[DOWN_MACD_INDEX_T0:UP_MACD_INDEX_T1])
            print("DIFF_MIN1:", DIFF_MIN1)

            DIFF_MIN2 = 0
            DIFF_MIN2 = min(quick_line[DOWN_MACD_INDEX_T2:UP_MACD_INDEX_T3])
            print("DIFF_MIN2:", DIFF_MIN2)

            if DIFF_MIN1 > DIFF_MIN2:
                print("当前状态为{}， {}！！！".format(UP_NAME, UP_DIFF_NAME))
            else:
                print("当前状态为{}， 但没有出现 {}！！！".format(UP_NAME, UP_DIFF_NAME))

            ################## 【MACD柱底背离】##################
            MACD_MIN1 = 0
            MACD_MIN1 = min(_data_volume[DOWN_MACD_INDEX_T0:UP_MACD_INDEX_T1])
            print("MACD_MIN1:", MACD_MIN1)

            MACD_MIN2 = 0
            MACD_MIN2 = min(_data_volume[DOWN_MACD_INDEX_T2:UP_MACD_INDEX_T3])
            print("MACD_MIN2:", MACD_MIN2)

            if MACD_MIN1 > MACD_MIN2:
                print("当前状态为{}， 【MACD柱底背离】！！！".format(UP_NAME))
            else:
                print("当前状态为{}， 但没有出现 【MACD柱顶背离】！！！".format(UP_NAME))


            ################## 【MACD柱面积顶背离】##################
            MACD_AREA_SUM1 = 0
            MACD_AREA_SUM1 = np.sum(_data_volume[DOWN_MACD_INDEX_T0:UP_MACD_INDEX_T1])
            print("MACD_AREA_SUM1:", MACD_AREA_SUM1)

            MACD_AREA_SUM2 = 0
            MACD_AREA_SUM2 = np.sum(_data_volume[DOWN_MACD_INDEX_T2:UP_MACD_INDEX_T3])
            print("MACD_AREA_SUM2:", MACD_AREA_SUM2)

            if MACD_AREA_SUM1 > MACD_AREA_SUM2:
                print("当前状态为{}， 【MACD柱面积顶背离】！！！".format(UP_NAME))
            else:
                print("当前状态为{}， 但没有出现 【MACD柱面积顶背离】！！！".format(UP_NAME))



        else:
            print("当前状态为金叉， 但是 MACD 不是底背离！！！")

    print("结束程序！！！")




