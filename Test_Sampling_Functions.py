import pandas as pd
import numpy as np

def func1(data, col, num):
    new_data = data.sample(num)
    return new_data[col]

def func2(data, col, num):
    new_data = data[col]
    new_data2 = new_data.sample(num)
    return new_data2

def func3(data, col, num):
    new_data = data[col]
    size = len(data.index)
    if size == 1:
        return [new_data[0] for i in range(num)]
    c = np.random.choice(len(data.index)-1, size = num)
    return [new_data[i] for i in c]

def func4(data, col, num):
    new_data = data[col]
    size = len(data.index)
    result = []
    for i in range(num):
        index = int((size-1) * np.random.random())
        result.append(new_data[index])
    return result