def EMA(r, days, name=0):
    if name == 'o+h+l+c':
        cps = [(v['Open'] + v['High'] + v['Low'] + v['Close']) / 4 for v in r]

    elif name == 'h+l+c':
        cps = [(v['High'] + v['Low'] + v['Close']) / 3 for v in r]

    elif name == 'h+l':
        cps = [(v['High'] + v['Low']) / 2 for v in r]

    else:
        cps = [v[name] for v in r] if name else r

    emas = [0 for i in range(len(cps))]  # 创造一个和cps一样大小的集合
    for i in range(len(cps)):
        if i == 0:
            emas[i] = cps[i]
        if i > 0:
            emas[i] = round(((days - 1) * emas[i - 1] + 2 * cps[i]) / (days + 1), 2)

    emas = [v for v in emas]
    return emas


if __name__ == '__main__':

    X = [3, 6, 8, 4, 9, 12, 16, 8, 10]

    r = [
        {
            'Time': 0,
            'Close': 0,
            'Open': 0,
            'High': 0,
            'Low': 0,
            'Volume': 0,
        },
        {
            'Time': 0,
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },        {
            'Time': 0,
            'Close': 4.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': 0,
            'Close': 6.5,
            'Open': 6.5,
            'High': 8,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': 0,
            'Close': 5,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },

    ]


    N = 2

    print("EMA: ", EMA(r, N, 'Close'))

    print("EMA: ", EMA(X, N))

    # c = [1, 2, 3, 4, 5, 6, 7]
    # print(EMA(c, 7))
    # # print((2 * 3) / 2)  # n = 1
    # # print((2 * 4 + 3) / 3)  # n = 2
    # # print((2 * 5 + 2 * 3.6666666666666665) / 4)  # n = 3
    # # print((2 * 6 + 3 * 4.333333333333333) / 5)  # n = 4
    # # print((2 * 7 + 4 * 5.0) / 6)  # n = 5
    #
    # print(EMA2(c, 7))