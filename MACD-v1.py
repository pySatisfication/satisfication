import math

import EMA

def print_min():
    # Use a breakpoint in the code line below to debug your script.
    m = 123
    n = 150
    print(f"m={m} n={n}")
    print(f"Min:", min(m, n))


def print_MACD():
    X_N = [3, -4, 5, 6, 7]
    X = [3, 4, 5, 6, 7]

    N = 5
    print(EMA.EMA(X, N))



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    X = [3, 6, 8, 4, 9, 12, 16, 8, 10]

    r = [
        {
            'Time': 0,
            'Close': 30,
            'Open': 0,
            'High': 0,
            'Low': 0,
            'Volume': 0,
        },
        {
            'Time': 0,
            'Close': 25.5,
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
        {
            'Time': 0,
            'Close': 5.5,
            'Open': 6.5,
            'High': 7,
            'Low': 4.5,
            'Volume': 100,
        }, {
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
            'Close': 8,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': 0,
            'Close': 10,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
        {
            'Time': 0,
            'Close': 7,
            'Open': 6.5,
            'High': 10,
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
        {
            'Time': 0,
            'Close': 3,
            'Open': 6.5,
            'High': 10,
            'Low': 4.5,
            'Volume': 100,
        },
    ]


    quick_period = 4
    slow_period = 2

    # quick_line_node = EMA.EMA(X, quick_period) - EMA.EMA(X, slowly_period)

    quick_line = EMA.EMA(r, quick_period, 'Close')
    print("quick:", quick_line)

    slow_line = EMA.EMA(r, slow_period, 'Close')
    print("slow:", slow_line)

    len_q = len(quick_line)

    # 顶背离

    macd_down = []
    macd_up = []
    for i in range(-1, len_q-1):
        b_macd_down = 0
        if (i > 1) and (quick_line[i-1] > slow_line[i-1]) and (quick_line[i] >= slow_line[i]) and (quick_line[i+1] < slow_line[i+1]):
            # 死叉
            b_macd_down = 1
        macd_down.append(b_macd_down)

        b_macd_up = 0
        if (i > 1) and (quick_line[i-1] < slow_line[i-1]) and (quick_line[i] <= slow_line[i]) and (quick_line[i+1] > slow_line[i+1]):
            # 死叉
            b_macd_up = 1
        macd_up.append(b_macd_up)

    print("macd_down:", macd_down)
    print("macd_up:", macd_up)