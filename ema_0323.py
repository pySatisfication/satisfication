import unittest
import numpy as np

def SLOPE(x, N):
    """
    描述：得到X的N周期的线型回归的斜率
    参数：
    1、N为有效值，但当前的k线数不足N根，该函数返回空值
    2、N为0时，该函数返回空值
    3、N为空值，该函数返回空值
    4、N可以为变量
    """
    if N is None:
        return None

    # 没有显式声明N<0时的处理策略，这里默认返回None
    if not isinstance(N, int) or N <= 0:
        return None

    assert isinstance(x, list), \
        'type {} does not match original type {}'.format(
        type(x), list)

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

def STD(x, N):
    """
    描述：求X在N个周期内的标准差
    参数：
    1、N为有效值，但当前的k线数不足N根，该函数返回空值；
    2、N为0时，该函数返回空值；
    3、N为空值，该函数返回空值。
    4、N可以为变量
    """
    if N is None:
        return None

    # 没有显式声明N<0时的处理策略，这里默认返回None
    if not isinstance(N, int) or N <= 0:
        return None

    assert isinstance(x, list), \
        'type {} does not match original type {}'.format(
        type(x), list)

    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        return None

    return np.std(x[len(x)-N:])

def MA(x, N):
    """
    描述：简单移动平均线沿用最简单的统计学方式，将过去某特定时间内的价格取其平均值
    参数：
    1、当N为有效值，但当前的k线数不足N根，函数返回空值
    2、N为0或空值的情况下，函数返回空值
    3、N可以为变量
    """
    if N is None:
        return None

    # 没有显式声明N<0时的处理策略，这里默认返回None
    if not isinstance(N, int) or N <= 0:
        return None

    assert isinstance(x, list), \
        'type {} does not match original type {}'.format(
        type(x), list)

    # 当前的k线数不足N根，函数返回空值
    if len(x) < N:
        return None

    return np.mean(x[len(x)-N:])

def SMA(x, N, M):
    """
    描述：求X的N个周期内的移动平均，M为权重
    计算公式：SMA(N)=SMA(N-1)*(N-M)/N+X(N)*M/N
    参数：
    1、当N为有效值，但当前的k线数不足N根，按实际根数计算
    2、N为0或空值的情况下，函数返回空值
    """
    if N is None:
        return None

    # 没有显式声明N<0时的处理策略，这里默认返回None
    if not isinstance(N, int) or N <= 0:
        return None

    assert isinstance(x, list), \
        'type {} does not match original type {}'.format(
        type(x), list)

    # 当前的k线数不足N根，按实际根数计算
    if len(x) < N:
        N = len(x)

    if N == 1:
        return x[-1]

    return SMA(x[0:-1], N - 1, M) * (N - M) / N + x[-1] * M / N

def EMA(x, N):
    """
    描述：求N周期X值的指数移动平均（平滑移动平均）。
    参数
    1、对距离当前较近的k线赋予了较大的权重。
    2、当N为有效值，但当前的k线数不足N根，按实际根数计算。
    3、N为0或空值时返回值为空值。
    4、N可以为变量
    """
    if N is None:
        return None

    # 没有显式声明N<0时的处理策略，这里默认返回None
    if not isinstance(N, int) or N <= 0:
        return None

    assert isinstance(x, list), \
        'type {} does not match original type {}'.format(
        type(x), list)

    assert (type(x[-1]) is float or type(x[-1]) is int) and x[-1] >= 0, \
        'the element must be of type float or integer, and the value must be greater than 0, get {}'.format(x[-1])

    # 当前的k线数不足N根，按实际根数计算
    if len(x) < N:
        N = len(x)

    # N<=0时，上面的递归退出条件返回为None，所以不参与递归，手动终止
    if N == 1:
        return x[-1]

    return (2 * x[-1] + (N-1) * EMA(x[0:-1], N - 1)) / (N + 1)

class TestMai(unittest.TestCase):
    def setUp(self):
        self._x = [1, 2, 3, 4, 5]

    def test_std(self):
        self.assertEqual(STD(self._x, 'aa'), None)
        self.assertEqual(STD(self._x, None), None)
        self.assertEqual(STD(self._x, 0), None)
        self.assertEqual(STD(self._x, 6), None)

    def test_ma(self):
        self.assertEqual(MA(self._x, 'aa'), None)
        self.assertEqual(MA(self._x, None), None)
        self.assertEqual(MA(self._x, 0), None)
        self.assertEqual(MA(self._x, 6), None)

    def test_sma(self):
        self.assertEqual(SMA(self._x, 'aa', 1), None)
        self.assertEqual(SMA(self._x, None, 1), None)
        self.assertEqual(SMA(self._x, 0, 1), None)
        self.assertEqual(SMA(self._x, 6, 1), 3)

    def test_ema(self):
        self.assertEqual(EMA(self._x, 'aa'), None)
        self.assertEqual(EMA(self._x, None), None)
        self.assertEqual(EMA(self._x, 0), None)
        #self.assertEqual(EMA(self._x, 6), None)

if __name__ == '__main__':
    X = [6.5, 7.18, 8.96, 9.26, 11.5]

    print('-----calculation of real value for SLOPE-----')
    B = SLOPE(X, 5)
    print("regression parameter:", B)

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

    X = [1, 2, 3, 4, 5]

    print('-----calculation of real value for MA-----')
    print(MA(X, 1))
    print(MA(X, 4))
    print(MA(X, 5))
    #print(MA(X, 6))

    print('-----calculation of real value for SMA-----')
    print(SMA(X, 1, 1))
    print(SMA(X, 4, 1))
    print(SMA(X, 5, 1))
    print(SMA(X, 6, 1))

    print('-----calculation of real value for EMA-----')
    print(EMA(X, 1))
    print(EMA(X, 4))
    print(EMA(X, 5))
    print(EMA(X, 6))

    unittest.main()
