


def CROSS(arrX, arrY):


    assert isinstance(arrX, list), \
        '1.1 {}: type {} does not match original type {}'.format(__name__, type(arrX), list)
    assert isinstance(arrY, list), \
        '1.2 {}: type {} does not match original type {}'.format(__name__, type(arrY), list)


    len_arrX = len(arrX)
    len_arrY = len(arrY)

    assert len_arrX >= 2, \
        '1.3 {}: the len of array  must be greater than 2, get {}'.format(__name__, len_arrX)
    assert len_arrY >= 2, \
        '1.4 {}: the len of array  must be greater than 2, get {}'.format(__name__, len_arrY)

    assert len_arrX == len_arrY, \
        '1.4 {}: the len of array X  must be equal to len of array Y, len X {} , len Y {}'.format(__name__, len_arrX, len_arrY)

    n = len_arrX
    if arrX[n-1] > arrY[n-1] and arrX[n-2] <= arrY[n-2]:
        return 1
    else:
        return 0
    pass

if __name__ == '__main__':

    X = [3, 15, 5, 9, 7]
    Y = [2, 6, 3, 8, 9]
    print("X：", X)
    print("Y：", Y)
    print("CROSS：", CROSS(X, Y))

    Y = [3, 15, 5, 9, 7]
    X = [2, 6, 3, 8, 9]
    print("X：", X)
    print("Y：", Y)
    print("CROSS：", CROSS(X, Y))

    Y = [3, 15, 5, 8, 7]
    X = [2, 6, 3, 8, 9]
    print("X：", X)
    print("Y：", Y)
    print("CROSS：", CROSS(X, Y))