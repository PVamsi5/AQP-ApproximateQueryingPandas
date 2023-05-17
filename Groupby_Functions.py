import pandas as pd
import numpy as np
import time

# Uniform sampling functions

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

# Stratified sampling functions

def aqp_groupby_strat_mean(group_data, data, col, time) :
    if isinstance(group_data, pd.core.groupby.generic.DataFrameGroupBy) == 0:
        raise Exception("Expecting a Pandas DataFrameGroupBy - group_data")
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int(time/(0.000008))
    result = {}
    result[col] = []
    result["result"] = []
    for key, value in group_data.groups.items():
        temp = []
        curr_size = len(value)
        curr_sample_size = max(1,int((sample_size*curr_size)/size))
        for i in range(curr_sample_size):
            index = int((curr_size-1) * np.random.random())
            temp.append(new_data[value[index]])
        result["result"].append(sum(temp)/curr_sample_size)
        result[col].append(key)
    return pd.DataFrame(result)

def aqp_groupby_strat_sum(group_data, data, col, time) :
    if isinstance(group_data, pd.core.groupby.generic.DataFrameGroupBy) == 0:
        raise Exception("Expecting a Pandas DataFrameGroupBy - group_data")
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int(time/(0.000008))
    result = {}
    result[col] = []
    result["result"] = []
    for key, value in group_data.groups.items():
        temp = []
        curr_size = len(value)
        curr_sample_size = max(1,int((sample_size*curr_size)/size))
        for i in range(curr_sample_size):
            index = int((curr_size-1) * np.random.random())
            temp.append(new_data[value[index]])
        result["result"].append((sum(temp)/curr_sample_size)*curr_size)
        result[col].append(key)
    return pd.DataFrame(result)

def aqp_groupby_strat_count(group_data, data, col, time) :
    if isinstance(group_data, pd.core.groupby.generic.DataFrameGroupBy) == 0:
        raise Exception("Expecting a Pandas DataFrameGroupBy - group_data")
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    size = len(data.index)
    new_data = data[col]
    sample_size = int(time/(0.000008))
    result = {}
    result[col] = []
    result["result"] = []
    for key, value in group_data.groups.items():
        temp = []
        curr_size = len(value)
        curr_sample_size = max(1,int((sample_size*curr_size)/size))
        for i in range(curr_sample_size):
            index = int((curr_size-1) * np.random.random())
            temp.append(new_data[value[index]])
        result["result"].append((sum(~np.isnan(temp))/curr_sample_size)*curr_size)
        result[col].append(key)
    return pd.DataFrame(result)

def aqp_groupby_strat_agg(group_data, data, col, agg, time) :
    if isinstance(group_data, pd.core.groupby.generic.DataFrameGroupBy) == 0:
        raise Exception("Expecting a Pandas DataFrameGroupBy - group_data")
    if isinstance(data, pd.DataFrame) == 0:
        raise Exception("Expecting a Pandas Dataframe - data")
    if isinstance(time, int) == 0 and isinstance(time, float) == 0:
        raise Exception("Expection a real number - time")
    trail = agg([0])
    if isinstance(trail, int) == 0 and isinstance(trail, float) == 0:
        raise Exception("Aggregate function doesnot return a number for a given list")
    size = len(data.index)
    new_data = data[col]
    sample_size = int(time/(0.000008))
    result = {}
    result[col] = []
    result["result"] = []
    for key, value in group_data.groups.items():
        temp = []
        curr_size = len(value)
        curr_sample_size = max(1,int((sample_size*curr_size)/size))
        for i in range(curr_sample_size):
            index = int((curr_size-1) * np.random.random())
            temp.append(new_data[value[index]])
        result["result"].append(agg(temp))
        result[col].append(key)
    return pd.DataFrame(result)

def aqp_groupby_strat_min(group_data, data, col, time):
    return aqp_groupby_strat_agg(group_data, data, col, min, time)

def aqp_groupby_strat_max(group_data, data, col, time):
    return aqp_groupby_strat_agg(group_data, data, col, max, time)

def aqp_groupby_strat_std(group_data, data, col, time):
    return aqp_groupby_strat_agg(group_data, data, col, np.std, time)

def aqp_groupby_strat_var(group_data, data, col, time):
    return aqp_groupby_strat_agg(group_data, data, col, np.var, time)