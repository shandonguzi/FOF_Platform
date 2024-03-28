

import pandas as pd
import numpy as np

def get_row_top_underlying(x, top_num):
    data_tmp = x.nlargest(top_num).dropna().index.values
    if len(data_tmp) < top_num:
        data = np.full(10, np.nan)
        data[:len(data_tmp)] = data_tmp
    else:
        data = data_tmp
    index = np.char.add('Top ', (np.arange(top_num) + 1).astype(str))
    result = pd.Series(data, index=index)
    return result

def select_top_underlying(factor_matrix, top_num):
    factor_matrix = factor_matrix.loc[pd.notna(factor_matrix).sum(axis=1) > top_num]
    underlying = factor_matrix.apply(lambda x: get_row_top_underlying(x, top_num) , axis=1)
    underlying.index = underlying.index + pd.offsets.MonthBegin(0)
    return underlying
