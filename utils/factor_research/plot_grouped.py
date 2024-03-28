
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'FZHei-B01S'
plt.rcParams['axes.unicode_minus'] = False

def select_underlying(factor_matrix, groups):
    grouped_underlying = factor_matrix.apply(lambda x: pd.qcut(x, groups, labels=np.arange(groups)) if pd.notna(x).sum() >= groups else pd.Series([np.nan] * len(x), x.index), axis=1).dropna(how='all')
    grouped_underlying = grouped_underlying.apply(lambda x: pd.Series([(x[x == rank]).index.values for rank in range(groups)], index=range(groups)), axis=1)
    return grouped_underlying

def get_grouped_underlying_return(date, funds, ranks, return_matrix):

    if date == pd.to_datetime('today').normalize() + pd.offsets.MonthEnd(0):
        return pd.Series([np.nan] * len(funds), index = ranks)
    else:
        underlying_return = pd.Series([return_matrix.loc[date, funds[rank]].mean() for rank in ranks])
        return underlying_return

def get_grouped_underlying_return_series(return_matrix, factor_matrix, groups):
    grouped_underlying = select_underlying(factor_matrix, groups)
    grouped_funds_return = grouped_underlying.apply(lambda x: get_grouped_underlying_return(x.name + pd.offsets.MonthEnd(0), x.values, x.index, return_matrix), axis=1)
    return grouped_funds_return


def get_cumulative_group_return(return_matrix, factor_matrix, groups, start_date=None):
    underlying_return_series = get_grouped_underlying_return_series(return_matrix, factor_matrix.loc[: pd.to_datetime('today').normalize() - pd.offsets.MonthBegin(1)], groups)
    if start_date:
        underlying_return_series = underlying_return_series.loc[start_date: ]
    cumulative_return = (underlying_return_series + 1).cumprod()
    cumulative_return.loc[cumulative_return.index[0] - pd.offsets.MonthBegin(0)] = 1
    cumulative_return = cumulative_return.add_prefix('Group ')
    cumulative_return = cumulative_return.rename(columns={'Group 0': 'Group 0 Min Factor', f'Group {groups - 1}': f'Group {groups - 1} Max Factor'})
    return cumulative_return

def plot_group_underlying_return(return_matrix, factor_matrix, groups, start_date=None):
    cumulative_return = get_cumulative_group_return(return_matrix, factor_matrix, groups, start_date)
    cumulative_return.plot()
    plt.legend()
