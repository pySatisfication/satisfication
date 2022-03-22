
import math

def print_abs():
    # Use a breakpoint in the code line below to debug your script.
    m = abs(123)
    n = abs(-150)
    print(f"ABS: m={m} n={n}")
# numpy.abs(x) or numpy.absolute(x)

def print_max():
    # Use a breakpoint in the code line below to debug your script.
    m = 123
    n = 150
    print(f"m={m} n={n}")
    print(f"MAX:", max(m, n))

def print_min():
    # Use a breakpoint in the code line below to debug your script.
    m = 123
    n = 150
    print(f"m={m} n={n}")
    print(f"Min:", min(m, n))


def print_sqrt():
    n = 9
    print(f"n={n}")
    print(f"math sqrt:", math.sqrt(n))



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_abs()
    print_max()
    print_min()
    print_sqrt()
