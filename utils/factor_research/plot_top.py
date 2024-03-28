
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'FZHei-B01S'
plt.rcParams['axes.unicode_minus'] = False

def select_underlying(factor_matrix, top_num):
    underlying = factor_matrix.apply(lambda x: pd.Series(x.nlargest(top_num).index.values, index=np.char.add('Top ', (np.arange(top_num) + 1).astype(str))) , axis=1)
    underlying.index = underlying.index + pd.offsets.MonthBegin(0)
    return underlying

def get_top_underlying_return(date, funds, ranks, return_matrix):
    if date == pd.to_datetime('today').normalize() + pd.offsets.MonthEnd(0):
        return pd.Series([np.nan] * len(funds), index = ranks)
    else:
        underlying_return = return_matrix.loc[date, funds]
        underlying_return.index = ranks
        return underlying_return

def get_top_underlying_return_series(return_matrix, factor_matrix, top_num):
    top_underlying = select_underlying(factor_matrix, top_num)
    top_funds_return = top_underlying.apply(lambda x: get_top_underlying_return(x.name + pd.offsets.MonthEnd(0), x.values, x.index, return_matrix), axis=1)
    return top_funds_return

def plot_top_underlying_return(return_matrix, factor_matrix, top_num, label, start_date=None):
    underlying_return_series = get_top_underlying_return_series(return_matrix, factor_matrix.loc[: pd.to_datetime('today').normalize() - pd.offsets.MonthBegin(1)], top_num).mean(axis=1) + 1
    if start_date:
        underlying_return_series = underlying_return_series.loc[start_date: ]
    cumulative_return = underlying_return_series.cumprod()
    cumulative_return.loc[cumulative_return.index[0] - pd.offsets.MonthBegin(0)] = 1
    cumulative_return.plot(label=label)
    plt.legend()
