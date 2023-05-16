import pandas as pd
import numpy as np
import time

def aqp_groupby_mean(data, col1, col2, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col1]
    new_data2 = data[col2]
    sample_size = int((time)/(0.000008))
    result = {}
    result[col1] = []
    result["result"] = []
    temp = {}
    for i in range(sample_size):
        index = int((size-1) * np.random.random())
        try:
            temp[new_data[index]].append(new_data2[index])
        except:
            temp[new_data[index]] = [new_data2[index]]
    for key in sorted(temp.keys()):
        values = temp[key]
        result[col1].append(key)
        result["result"].append(sum(values)/len(values))
    return pd.DataFrame(result)

def aqp_groupby_sum(data, col1, col2, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col1]
    new_data2 = data[col2]
    sample_size = int((time)/(0.000008))
    result = {}
    result[col1] = []
    result["result"] = []
    temp = {}
    for i in range(sample_size):
        index = int((size-1) * np.random.random())
        try:
            temp[new_data[index]].append(new_data2[index])
        except:
            temp[new_data[index]] = [new_data2[index]]
    for key in sorted(temp.keys()):
        values = temp[key]
        curr_len = len(values)
        result[col1].append(key)
        curr_sum = (sum(values)/curr_len) * (curr_len/sample_size) * size
        result["result"].append(curr_sum)
    return pd.DataFrame(result)

def aqp_groupby_count(data, col1, col2, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col1]
    new_data2 = data[col2]
    sample_size = int((time)/(0.000008))
    result = {}
    result[col1] = []
    result["result"] = []
    temp = {}
    for i in range(sample_size):
        index = int((size-1) * np.random.random())
        try:
            temp[new_data[index]].append(new_data2[index])
        except:
            temp[new_data[index]] = [new_data2[index]]
    for key in sorted(temp.keys()):
        values = ~np.isnan(temp[key])
        curr_len = len(values)
        result[col1].append(key)
        curr_sum = (sum(values)/curr_len) * (curr_len/sample_size) * size
        result["result"].append(curr_sum)
    return pd.DataFrame(result)

def aqp_groupby_agg(data, col1, col2, agg, time):
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    trail = agg([0])
    if isinstance(trail, int) == 0 and isinstance(trail, float) == 0:
        raise Exception("Aggregate function doesnot return a number for a given list")
    size = len(data.index)
    new_data = data[col1]
    new_data2 = data[col2]
    sample_size = int((time)/(0.000008))
    result = {}
    result[col1] = []
    result["result"] = []
    temp = {}
    for i in range(sample_size):
        index = int((size-1) * np.random.random())
        try:
            temp[new_data[index]].append(new_data2[index])
        except:
            temp[new_data[index]] = [new_data2[index]]
    for key in sorted(temp.keys()):
        result[col1].append(key)
        result["result"].append(agg(temp[key]))
    return pd.DataFrame(result)

def aqp_groupby_min(data, col1, col2, time):
    return aqp_groupby_agg(data, col1, col2, min, time)

def aqp_groupby_max(data, col1, col2, time):
    return aqp_groupby_agg(data, col1, col2, max, time)

def aqp_groupby_std(data, col1, col2, time):
    return aqp_groupby_agg(data, col1, col2, np.std, time)

def aqp_groupby_var(data, col1, col2, time):
    return aqp_groupby_agg(data, col1, col2, np.var, time)
