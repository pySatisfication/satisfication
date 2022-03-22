
def partition(arr, low, high):
    i = (low - 1)         # 最小元素索引
    pivot = arr[high]

    for j in range(low, high):
        # 当前元素小于或等于 pivot
        if arr[j] <= pivot:
            i = i + 1
            arr[i], arr[j] = arr[j], arr[i]

    arr[i + 1], arr[high] = arr[high], arr[i + 1]

    return (i + 1)


# 快速排序函数
def quick_sort(arr, low, high):
    if low < high:
        pi = partition(arr, low, high)
        quick_sort(arr, low, pi - 1)
        quick_sort(arr, pi + 1, high)


def EMA(x, N):
    assert isinstance(N, int), \
        '1.1 EMA: type {} does not match original type {}'.format(
        type(N), int)

    if N <= 0:
        return None

    assert isinstance(x, list), \
        '1.2 EMA: type {} does not match original type {}'.format(
        type(x), list)

    assert type(x[-1]) is int and x[-1] >= 0, \
        'the element must be of type integer and the value must be greater than 0, get {}'.format(x[-1])

    if len(x) < N:
        N = len(x)

    if N == 1:
        return x[len(x) - N]





    return (2 * x[-1] + (N-1) * EMA(x[0:-1], N - 1)) / (N + 1)

if __name__ == '__main__':


    # X = [3, 4, 5, 6, 7]
    X = [3, 15, 5, 9, 7]

    N = 5

    print("MAX：", max(X))

    print("quick sort：", quick_sort(X, 2, 4))


    #print(EMA(X, N))
    #print(EMA(X, None))
    #print(EMA(X, 0))
    #print(EMA(X, 1))
    #print(EMA(X, 4))
    #print(EMA(X, 5))
    #print(EMA(X, 6))
    #print(EMA(X, 10))
