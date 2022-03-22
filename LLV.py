

def LLV(arr, N):
    str_func = "LLV"

    assert isinstance(N, int), \
        '1.1 {}: type {} does not match original type {}'.format(str_func, type(N), int)

    if N <= 0:
        return None

    assert isinstance(arr, list), \
        '1.2 {}: type {} does not match original type {}'.format(str_func, type(arr), list)

    len_arr = len(arr)
    if len_arr < N:
        N = len_arr

    if arr:
      arr_new = arr[-N:]
      # print("LLV：", arr_hhv)
      ret = min(arr_new)
      return ret
    else:
      print('1.3 {}: empty array {}'.format(str_func, arr))
      return


if __name__ == '__main__':

    # X = [3, 4, 5, 6, 7]
    X = [3, 15, 5, 9, 7]
    # X = []
    N = 5

    print("LLV：", LLV(X, 4))
    print("LLV：", LLV(X, 2))

    #print(EMA(X, N))
    #print(EMA(X, None))

    #print(EMA(X, 5))
    #print(EMA(X, 6))
    #print(EMA(X, 10))
