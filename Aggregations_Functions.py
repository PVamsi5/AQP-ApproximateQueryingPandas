import pandas as pd
import numpy as np
import time

# Online aggregation based AQP functions

def aqp_mean(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    sample_size = int(time/0.000008)
    size = len(data.index)
    new_data = data[col]
    new_data2 = sampling_fn(new_data,sample_size)
    result = sum(new_data2)/sample_size
    error = 1.96 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, error

def aqp_sum(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    sample_size = int(time/0.000008)
    size = len(data.index)
    new_data = data[col]
    new_data2 = sampling_fn(new_data,sample_size)
    result = size * (sum(new_data2)/sample_size)
    error = 1.96 * size * (np.std(new_data2)/np.sqrt(sample_size))
    return result, error

def aqp_agg(data, col, agg, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    trail = agg([0])
    if isinstance(trail, int) == 0 and isinstance(trail, float) == 0:
        raise Exception("Aggregate function doesnot return a number for a given list")
    size = len(data.index)
    new_data = data[col]
    sample_size = int((time/0.000008)/10)
    new_data2 = []
    for i in range(10):
        sample = sampling_fn(new_data,sample_size)
        new_data2.append(agg(sample))
    result = sum(new_data2)/10
    error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    return result, error

def aqp_min(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int((time/0.000008)/10)
    new_data2 = []
    for i in range(10):
        sample = sampling_fn(new_data,sample_size)
        new_data2.append(min(sample))
    result = sum(new_data2)/10
    error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    return result, error

def aqp_max(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int((time/0.000008)/10)
    new_data2 = []
    for i in range(10):
        sample = sampling_fn(new_data,sample_size)
        new_data2.append(max(sample))
    result = sum(new_data2)/10
    error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    return result, error

def aqp_count(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int(time/0.000008)
    new_data2 = sampling_fn(new_data,sample_size)
    new_data2 = ~np.isnan(new_data2)
    result = int(size * (sum(new_data2)/sample_size))
    error = 1.96 * size * (np.std(new_data2)/np.sqrt(sample_size))
    return result, error

def aqp_std(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int((time/0.000008)/10)
    new_data2 = []
    for i in range(10):
        sample = sampling_fn(new_data,sample_size)
        new_data2.append(np.std(sample))
    result = sum(new_data2)/10
    error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    return result, error

def aqp_var(data, col, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int((time/0.000008)/10)
    new_data2 = []
    for i in range(10):
        sample = sampling_fn(new_data,sample_size)
        new_data2.append(np.var(sample))
    result = sum(new_data2)/10
    error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    return result, error

# Error Bounded AQP aggregate Functions

def aqp_mean_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 0
    result = 0
    curr_error = 0
    start = time.time() 
    while 1:
        new_data2 += sampling_fn(new_data,1000)
        sample_size += 1000
        result = sum(new_data2)/sample_size
        curr_error = 1.96 * (np.std(new_data2)/np.sqrt(sample_size))
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
    return result, curr_error

def aqp_sum_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 0
    result = 0
    curr_error = 0
    start = time.time()
    while 1:
        new_data2 += sampling_fn(new_data,1000)
        sample_size += 1000
        result = size * (sum(new_data2)/sample_size)
        curr_error = 1.96 * size * (np.std(new_data2)/np.sqrt(sample_size))
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
    return result, curr_error

def aqp_agg_error(data, col, agg, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    trail = agg([0])
    if isinstance(trail, int) == 0 and isinstance(trail, float) == 0:
        raise Exception("Aggregate function doesnot return a number for a given list")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 10
    start = time.time()
    for i in range(10):
        sample = func4(data,col,1000)
        new_data2.append(agg(sample))
    result = sum(new_data2)/10
    curr_error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    while 1:
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
        new_data2.append(agg(sampling_fn(new_data,1000)))
        sample_size += 1
        result = (sum(new_data2)/sample_size)
        curr_error = 2.228 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, curr_error

def aqp_min_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 10
    start = time.time()
    for i in range(10):
        sample = func4(data,col,1000)
        new_data2.append(min(sample))
    result = sum(new_data2)/10
    curr_error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    while 1:
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
        new_data2.append(min(sampling_fn(new_data,1000)))
        sample_size += 1
        result = (sum(new_data2)/sample_size)
        curr_error = 2.228 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, curr_error

def aqp_max_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 10
    start = time.time()
    for i in range(10):
        sample = func4(data,col,1000)
        new_data2.append(max(sample))
    result = sum(new_data2)/10
    curr_error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    while 1:
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
        new_data2.append(max(sampling_fn(new_data,1000)))
        sample_size += 1
        result = (sum(new_data2)/sample_size)
        curr_error = 2.228 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, curr_error

def aqp_count_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 0
    result = 0
    curr_error = 0
    start = time.time()
    while 1:
        new_data2 += list(~np.isnan(sampling_fn(new_data,1000)))
        sample_size += 1000
        result = size * (sum(new_data2)/sample_size)
        curr_error = 1.96 * size * (np.std(new_data2)/np.sqrt(sample_size))
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
    return int(result), curr_error

def aqp_std_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 10
    start = time.time()
    for i in range(10):
        sample = func4(data,col,1000)
        new_data2.append(np.std(sample))
    result = sum(new_data2)/10
    curr_error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    while 1:
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
        new_data2.append(np.std(sampling_fn(new_data,1000)))
        sample_size += 1
        result = (sum(new_data2)/sample_size)
        curr_error = 2.228 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, curr_error

def aqp_var_error(data, col, error):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(error, int) == 0 and isinstance(error, float) == 0:
        raise Exception("Expection a real number - error")
    if error < 0 or error > 100:
        raise Exception("Error percentage can only be in between 0 and 100")
    size = len(data.index)
    new_data = data[col]
    new_data2 = []
    sample_size = 10
    start = time.time()
    for i in range(10):
        sample = func4(data,col,1000)
        new_data2.append(np.var(sample))
    result = sum(new_data2)/10
    curr_error = 2.262 * (np.std(new_data2)/np.sqrt(10))
    while 1:
        if (curr_error/result <= error/100) or (time.time() - start > 1.5):
            break
        new_data2.append(np.var(sampling_fn(new_data,1000)))
        sample_size += 1
        result = (sum(new_data2)/sample_size)
        curr_error = 2.228 * (np.std(new_data2)/np.sqrt(sample_size))
    return result, curr_error