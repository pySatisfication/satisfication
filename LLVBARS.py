import operator


def LLVBARS(arr, N):

    assert isinstance(N, int), \
        '1.1 {}: type {} does not match original type {}'.format(__name__, type(N), int)
    if N <= 0:
        return None
    assert isinstance(arr, list), \
        '1.2 {}: type {} does not match original type {}'.format(__name__, type(arr), list)

    len_arr = len(arr)
    if len_arr < N:
        N = len_arr
    if arr:
      arr_tmp = arr[-N:]
      len_arr_hhv = len(arr_tmp)
      min_index, min_number = min(enumerate(arr_tmp), key=operator.itemgetter(1))
      cycle = len_arr_hhv - min_index
      # print("LLVBARS：", arr_tmp)
      # print("max_index：", max_index)
      # print("cycle location to current：", N - min_index)
      # print("min_number：", min_number)

      return cycle
    else:
      print('1.3 {}: empty array {}'.format(__name__, arr))
      return None


if __name__ == '__main__':

    # X = [3, 4, 5, 6, 7]
    X = [3, 15, 5, 9, 7]
    # X = []
    N = 5

    print("HHVBARS：", LLVBARS(X, 4))


    #print(EMA(X, N))
    #print(EMA(X, None))

    #print(EMA(X, 5))
    #print(EMA(X, 6))
    #print(EMA(X, 10))
