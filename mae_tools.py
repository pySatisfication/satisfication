
import math

def print_abs():

    pass


def print_sqrt():
    n = 9
    print(f"n={n}")
    print(f"math sqrt:", math.sqrt(n))

    pass
    

def print_mae(target_list, prediction_list):
   
    error = []
    for i in range(len(target)):
        error.append(target[i] - prediction[i])
        
    print("Errors: ", error)
  
    squaredError = []  
    absError = []
    for val in error:
        absError.append(abs(val))  # 误差绝对值
    print("Absolute Value of Error: ", absError)
   
    ret_val = sum(absError) / len(absError)
    
    print("MAE = ", ret_val)  # 平均绝对误差MAE
    
    return ret_val
    
    pass
    
def print_RMAE(target_list, prediction_list):
   
    error = []
    for i in range(len(target)):
        error.append(target[i] - prediction[i])
        
    print("Errors: ", error)
  
    squaredError = []  
    absError = []
    for val in error:
        squaredError.append(val * val)  # target-prediction之差平方
    
    print("Square Error: ", squaredError)
    ret_val = sqrt(sum(squaredError) / len(squaredError))
    
    print("RMSE = ", ret_val)  # 均方根误差RMSE
    
    return ret_val
    
    pass   

if __name__ == '__main__':

    target = [1.5, 2.1, 3.3, -4.7, -2.3, 0.75]
    prediction = [0.5, 1.5, 2.1, -2.2, 0.1, -0.5]

    error = []
    for i in range(len(target)):
        error.append(target[i] - prediction[i])

    print("Errors: ", error)

    # print(error)

    squaredError = []
    absError = []
    for val in error:
        squaredError.append(val * val)  # target-prediction之差平方
        absError.append(abs(val))  # 误差绝对值

    print("Square Error: ", squaredError)
    print("Absolute Value of Error: ", absError)

    print("MSE = ", sum(squaredError) / len(squaredError))  # 均方误差MSE

    from math import sqrt

    print("RMSE = ", sqrt(sum(squaredError) / len(squaredError)))  # 均方根误差RMSE
    print("MAE = ", sum(absError) / len(absError))  # 平均绝对误差MAE

    targetDeviation = []
    targetMean = sum(target) / len(target)  # target平均值
    for val in target:
        targetDeviation.append((val - targetMean) * (val - targetMean))
    print("Target Variance = ", sum(targetDeviation) / len(targetDeviation))  # 方差

    print("Target Standard Deviation = ", sqrt(sum(targetDeviation) / len(targetDeviation)))  # 标准差
