def EMA(c, N):
    Y = 0
    n = 1
    for ci in c[-N:]:
        Y = (2 * ci + (n - 1) * Y) / (n + 1)
        n += 1
    return Y


def EMA2(c, N, denominator=1):
    if N >= 1:
        if denominator == 1:
            denominator = sum(range(N + 1))
        return N / denominator * c[-1] + EMA2(c[len(c) - N:len(c) - 1], N - 1, denominator)
    else:
        return 0


if __name__ == '__main__':

    X = [3, 4, 5, 6, 7]

    N = 5
    print("EMA: ",EMA(X, N))

    # c = [1, 2, 3, 4, 5, 6, 7]
    # print(EMA(c, 7))
    # # print((2 * 3) / 2)  # n = 1
    # # print((2 * 4 + 3) / 3)  # n = 2
    # # print((2 * 5 + 2 * 3.6666666666666665) / 4)  # n = 3
    # # print((2 * 6 + 3 * 4.333333333333333) / 5)  # n = 4
    # # print((2 * 7 + 4 * 5.0) / 6)  # n = 5
    #
    # print(EMA2(c, 7))