


def HHV(arr, N):


    assert isinstance(N, int), \
        '1.1 {}: type {} does not match original type {}'.format(__name__ , type(N), int)

    if N <= 0:
        return None

    assert isinstance(arr, list), \
        '1.2 {}: type {} does not match original type {}'.format(__name__ , type(arr), list)

    len_arr = len(arr)
    if len_arr < N:
        N = len_arr


    if arr:

      arr_new = arr[-N:]
      # print("HHV_max：", arr_new)
      ret = max(arr_new)
      return ret
    else:
      print('1.1 {}: empty array {}'.format(__name__ , arr))
      return


if __name__ == '__main__':

    # X = [3, 4, 5, 6, 7]
    X = [3, 15, 5, 9, 7]
    # X = []
    N = 5

    print("HHV：", HHV(X, 4))


