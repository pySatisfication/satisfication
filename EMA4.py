import numpy as np

def _ema(arr,i=None):
    N = len(arr)
    α = 2/(N+1) #平滑指数
    i = N-1 if i is None else i
    if i==0:
        return arr[i]
    else:
        data = 0
        data += (α*arr[i]+(1-α)*EMA(arr,i-1))  #递归公式
        return data

# def _ema(arr):
#     N = len(arr)
#     α = 2/(N+1)
#     data = np.zeros(len(arr))
#     for i in range(len(data)):
#         data[i] = arr[i] if i==0 else α*arr[i]+(1-α)*data[i-1]  #从首开始循环
#     return data[-1]


# def EMA(arr, period=21):
#     data = np.full(arr.shape,np.nan)
#     for i in range(period-1,len(arr)):
#         data[i] = _ema(arr[i+1-period:i+1])
#     return data

def EMA(arr,period=21):
    data = np.full(arr.shape,np.nan)
    for i in range(period-1,len(arr)):
        data[i] = _ema(arr[i+1-period:i+1])
    return data


# def EMA(x, N = 21):
#     assert isinstance(N, int), \
#         '1.1 EMA: type {} does not match original type {}'.format(
#         type(N), int)
#
#     if N <= 0:
#         return None
#
#     # assert isinstance(x, list), \
#     #     '1.2 EMA: type {} does not match original type {}'.format(
#     #     type(x), list)
#     #
#     # assert type(x[-1]) is int and x[-1] >= 0, \
#     #     'the element must be of type integer and the value must be greater than 0, get {}'.format(x[-1])
#
#     if len(x) < N:
#         N = len(x)
#
#     # if N == 1:
#     #     return x[len(x) - N]
#     data = np.full(x.shape, np.nan)
#
#     for i in range(N-1, len(x)):
#         data[i] = _ema(x[i+1-N:i+1])
#
#     return data

if __name__ == '__main__':

    X_N = [3, -4, 5, 6, 7]
    X = [3, 4, 5, 6, 7, 3, 4, 5, 6, 7, 3, 4, 5, 6, 7,3, 4, 5, 6, 7,3, 4, 5, 6, 7, 3, 4, 5, 6, 7, 3, 4, 5, 6, 7]

    # arr_data = np.array([3, -4], [5, 6], [6, 7], [3, -4], [5, 6], [6, 7], [3, -4], [5, 6], [6, 7])
    arr_data = np.array([[3, 4], [5, 6], [6, 7], [3, 4], [5, 6], [6, 7], [3, 4], [5, 6], [6, 7]])

    print(arr_data.ndim)
    print(arr_data.shape)
    print(arr_data.size)

    N = 5
    print(EMA(arr_data, N))
    #print(EMA(X, None))
    #print(EMA(X, 0))
    #print(EMA(X, 1))
    #print(EMA(X, 4))
    #print(EMA(X, 5))
    #print(EMA(X, 6))
    #print(EMA(X, 10))
