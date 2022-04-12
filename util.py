# coding=utf-8
import numpy as np
import time
import argparse
import sys

#def parse_args():
#    """
#    Parse input arguments
#    """
#    parser = argparse.ArgumentParser(description='arguments for modular call')
#    parser.add_argument('--ema', dest='ema', help='test ema func',
#                        default='', type=str)
#    parser.add_argument('--slope', dest='slope', help='test slope func',
#                        default='', type=str)
#    parser.add_argument('--ma', dest='ma', help='test ma func',
#                        default='', type=str)
#    parser.add_argument('--sma', dest='sma', help='test sma func',
#                        default='', type=str)
#    parser.add_argument('--std', dest='std', help='test std func',
#                        default='', type=str)
#
#    if len(sys.argv) == 1:
#        parser.print_help()
#        sys.exit(1)
#
#    args = parser.parse_args()
#    return args

def op_wrapper(op):
    def func_op(*args, **kwargs):
        name = kwargs.setdefault('name', op.__name__)
        if len(args) < 2 or len(args) > 3:
            raise RuntimeError('incorrect number of input parameters: %s' % name) 
        elif len(args) == 2:
            x, N = args
        elif len(args) == 3:
            x, N, M = args

        if N is None:
            return None
        assert isinstance(N, int), \
            'type {} does not match original type {}'.format(
            type(N), int)
        if N <= 0:
            return None

        # check param x
        assert isinstance(x, list), \
            'type {} does not match original type {}'.format(
            type(x), list)
        if len(x) == 0:
            return None
        for item in x:
            if (not isinstance(item, float) and not isinstance(item, int)) or item < 0:
                raise TypeError('type {} does not match original type {} or value less than 0'.format(
                            type(item), 'float/integer'))

        if name in ['ma', 'ema', 'slope', 'std']:
            return op(x, N)
        if name in ['sma', 'boll']:
            if M is None:
                return None
            assert isinstance(M, int), \
                'type {} does not match original type {}'.format(
                type(M), int)
            if M <= 0:
                return None
            return op(x, N, M)
    return func_op

@op_wrapper
def slope(x, N):
    """
    描述：得到X的N周期的线型回归的斜率
    参数：
    1、N为有效值，但当前的k线数不足N根，该函数返回空值
    2、N为0时，该函数返回空值
    3、N为空值，该函数返回空值
    4、N可以为变量
    """
    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        return None

    b = np.ones(N)
    X = np.array([i + 1 for i in range(N)])
    X = np.array([item for item in zip(b,X)])
    Y = np.array(x)

    #X = np.array([i + 1 for i in range(N)])
    #Y = np.array(x)
    XTX = np.matmul(np.transpose(X),X)
    XTY = np.matmul(np.transpose(X),Y)
    B = np.matmul(np.linalg.inv(XTX), XTY)

    return B

@op_wrapper
def std(x, N):
    """
    描述：求X在N个周期内的标准差
    参数：
    1、N为有效值，但当前的k线数不足N根，该函数返回空值；
    2、N为0时，该函数返回空值；
    3、N为空值，该函数返回空值。
    4、N可以为变量
    """
    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        N = len(x)

    return np.std(x[len(x)-N:])

@op_wrapper
def boll(x, N=20, M=2):
    """
    描述：简单移动平均线沿用最简单的统计学方式，将过去某特定时间内的价格取其平均值
    参数：
    1、当N为有效值，但当前的k线数不足N根，函数返回空值
    2、N为0或空值的情况下，函数返回空值
    3、N可以为变量
    """
    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        N = len(x)
    boll_st = ma(x, N)

    ub_st = boll_st + M * std(x, N)
    lb_st = boll_st - M * std(x, N)

    return np.mean(x[len(x)-N:])

@op_wrapper
def ref(x, N):
    assert len(x) > 0

    if len(x) == 1:
        return None

    if len(x) < N + 1:
        N = len(x)
    return x[-(N+1)]

@op_wrapper
def ma(x, N):
    """
    描述：简单移动平均线沿用最简单的统计学方式，将过去某特定时间内的价格取其平均值
    参数：
    1、当N为有效值，但当前的k线数不足N根，函数返回空值
    2、N为0或空值的情况下，函数返回空值
    3、N可以为变量
    """
    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        return None

    return np.mean(x[len(x)-N:])

@op_wrapper
def sma(x, N, M):
    """
    描述：求X的N个周期内的移动平均，M为权重
    计算公式：SMA(N)=SMA(N-1)*(N-M)/N+X(N)*M/N
    参数：
    1、当N为有效值，但当前的k线数不足N根，按实际根数计算
    2、N为0或空值的情况下，函数返回空值
    """
    # 当前的k线数不足N根，按实际根数计算
    if len(x) < N:
        N = len(x)

    if N == 1:
        return x[-1]

    return sma(x[0:-1], N - 1, M) * (N - M) / N + x[-1] * M / N

def ema_alge(his_val, cur_val, N):
    assert N > 0
    return 2/(N+1) * cur_val + (N-1)/(N+1) * his_val

@op_wrapper
def ema(x, N):
    """
    描述：求N周期X值的指数移动平均（平滑移动平均）。
    参数
    1、对距离当前较近的k线赋予了较大的权重。
    2、当N为有效值，但当前的k线数不足N根，按实际根数计算。
    3、N为0或空值时返回值为空值。
    4、N可以为变量
    """
    ## 当前的k线数不足N根，按实际根数计算
    #if len(x) < N:
    #    N = len(x)

    if len(x) == 1:
        return round(x[-1], 2)
    if len(x) == 2:
        return round(2.0/(N+1)*x[-1]+float(N-1)/(N+1)*x[-2], 2)

    # real 
    K = len(x)
    ema_val = 0.0
    for idx in range(K):
        if idx == 0:
            ratio = 2.0/(N+1)
        elif idx == K - 1:
            ratio = np.power(float(N-1)/(N+1), idx)
        else: 
            ratio = 2.0/(N+1) * np.power(float(N-1)/(N+1), idx)
        ema_val += ratio * x[0-(idx+1)]
        
    return round(ema_val, 2)

def ema_recur(x, N):
    """
    描述：求N周期X值的指数移动平均（平滑移动平均）。
    参数
    1、对距离当前较近的k线赋予了较大的权重。
    2、当N为有效值，但当前的k线数不足N根，按实际根数计算。
    3、N为0或空值时返回值为空值。
    4、N可以为变量
    """
    ## 当前的k线数不足N根，按实际根数计算
    #if len(x) < N:
    #    N = len(x)

    if len(x) == 1:
        return round(x[-1], 2)
    if len(x) == 2:
        return round(2.0/(N+1)*x[-1]+float(N-1)/(N+1)*x[-2], 2)

    return round((2.0 * x[-1] + float(N-1) * ema_recur(x[0:-1], N)) / (N + 1), 2)

if __name__ == '__main__':
    __all_func__ = ['ma','ema','ema_recur','sma','std','slope','all']

    X = [26810,26030,25560,25870,24890,24725,25140,25025,24910,24870,24565,24930,25555,25275,25190,25570,25685,25380,25315,25445,26170,26900,26845,26705,27000,28330,29380,28980,28900,28915,28970]
    #X = [447.7,437.3,431,440.8,423.8,432.1,445.8,446.2,440.5,437.9,428.9,444.3,433.6,438.5]
    print("X:", X)
    print()
    for t in range(len(sys.argv) - 1):
        func = sys.argv[t + 1]
        if func not in __all_func__:
            continue

        if func == 'ema':
            print('-----ema-----')

            print("ema(X, 1)=", ema(X, 1))
            print("ema(X, 12)=", ema(X, 12))
            print("ema(X, 26)=", ema(X, 26))
            print("ema(X, 27)=", ema(X, 27))

            print('---cost of time---') 
            start = time.time() * 1000
            print("ema_recur(X, 26)=", ema_recur(X, 26))
            end = time.time() * 1000
            print("cost of ema_recur:", end - start)

            start = time.time() * 1000
            print("ema(X, 26)=", ema(X, 26))
            end = time.time() * 1000
            print("cost of ema:", end - start)

            print('---N=12---') 
            for day in range(len(X)):
                print("day {}: non-recursion: {}, recursion: {}".format(day+1, ema(X[:day+1], 12), ema_recur(X[:day+1], 12)))

            print('---N=26---') 
            for day in range(len(X)):
                print("day {}: non-recursion: {}, recursion: {}".format(day+1, ema(X[:day+1], 26), ema_recur(X[:day+1], 26)))

            print('-----ema-----')
            print()
        elif func == 'slope':
            #X = [6.5, 7.18, 8.96, 9.26, 11.5]
            X = [20.65, 20.01, 20.06, 20.69, 20.35]

            print('-----slope-----')
            B = []
            try:
                B = slope(X, 5)
            except AssertionError as e:
                print("assertion error:", e)
            print("regression parameter:", B)

            if len(B) > 0:
                import matplotlib.pyplot as plt
                
                # dot
                dot_x = np.array([i + 1 for i in range(5)])
                dot_y = X
                T = np.arctan2(dot_x, dot_y)
                plt.scatter(dot_x, dot_y, c=T, alpha=0.5)

                # line
                line_x = np.linspace(-2,10,50)
                line_y = B[1] * line_x + B[0]
                
                plt.plot(line_x, line_y)
                plt.show()
            print('-----slope-----')
            print()
        elif func == 'ma':
            print('-----ma-----')
            print(ma(X, 1))
            print(ma(X, 5))
            print(ma(X, 26))
            print(ma(X, 27))
            print('-----ma-----')
            print()
        elif func == 'sma':
            print('-----sma-----')
            print(sma(X, 1, 1))
            print(sma(X, 5, 1))
            print(sma(X, 26, 1))
            print(sma(X, 27, 1))
            print('-----sma-----')
            print()
        elif func == 'std':
            print('-----std-----')
            print(std(X, 1))
            print(std(X, 5))
            print(std(X, 26))
            print(std(X, 27))
            print('-----std-----')
            print()
        elif func == 'all':
            print('-----ema-----')
            print("ema(X, 1)=", ema(X, 1))
            print("ema(X, 12)=", ema(X, 12))
            print("ema(X, 26)=", ema(X, 26))
            print("ema(X, 27)=", ema(X, 27))

            start = time.time() * 1000
            print(ema_recur(X, 26))
            end = time.time() * 1000
            print("cost of ema_recur:", end - start)

            start = time.time() * 1000
            print(ema(X, 26))
            end = time.time() * 1000
            print("cost of ema:", end - start)
            print('-----ema-----')
            print()

            print('-----ma-----')
            print(ma(X, 1))
            print(ma(X, 4))
            print(ma(X, 5))
            print(ma(X, 6))
            print('-----ma-----')
            print()

            print('-----sma-----')
            print(sma(X, 1, 1))
            print(sma(X, 4, 1))
            print(sma(X, 5, 1))
            print(sma(X, 6, 1))
            print()

            print('-----std-----')
            print(std(X, 1))
            print(std(X, 5))
            print(std(X, 26))
            print(std(X, 27))
            print()

            print('-----slope-----')
            X = [6.5, 7.18, 8.96, 9.26, 11.5]
            print("X:", X)

            B = []
            try:
                B = slope(X, 5)
            except AssertionError as e:
                print("assertion error:", e)
            print("regression parameter:", B)
            print()

            if len(B) > 0:
                import matplotlib.pyplot as plt
                
                # dot
                dot_x = np.array([i + 1 for i in range(5)])
                dot_y = X
                T = np.arctan2(dot_x, dot_y)
                plt.scatter(dot_x, dot_y, c=T, alpha=0.5)

                # line
                line_x = np.linspace(-2,10,50)
                line_y = B[1] * line_x + B[0]
                
                plt.plot(line_x, line_y)
                plt.show()

