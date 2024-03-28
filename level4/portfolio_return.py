
import time

import numpy as np
import pandas as pd

from settings.database import level4_factor_result
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    top_funds_return = get_sql(level4_factor_result, 'top_funds_return', index_cols='Date')
    total_10_funds_weight_0 = get_sql(level4_factor_result, 'total_10_funds_weight_0', index_cols='Date')
    total_10_funds_weight_1 = get_sql(level4_factor_result, 'total_10_funds_weight_1', index_cols='Date')
    total_10_funds_weight_2 = get_sql(level4_factor_result, 'total_10_funds_weight_2', index_cols='Date')
    total_10_funds_weight_3 = get_sql(level4_factor_result, 'total_10_funds_weight_3', index_cols='Date')
    total_10_funds_weight_4 = get_sql(level4_factor_result, 'total_10_funds_weight_4', index_cols='Date')
    total_10_funds_weight_5 = get_sql(level4_factor_result, 'total_10_funds_weight_5', index_cols='Date')
    total_9_funds_weight_0 = get_sql(level4_factor_result, 'total_9_funds_weight_0', index_cols='Date')
    total_9_funds_weight_1 = get_sql(level4_factor_result, 'total_9_funds_weight_1', index_cols='Date')
    total_9_funds_weight_2 = get_sql(level4_factor_result, 'total_9_funds_weight_2', index_cols='Date')
    total_9_funds_weight_3 = get_sql(level4_factor_result, 'total_9_funds_weight_3', index_cols='Date')
    total_9_funds_weight_4 = get_sql(level4_factor_result, 'total_9_funds_weight_4', index_cols='Date')
    total_9_funds_weight_5 = get_sql(level4_factor_result, 'total_9_funds_weight_5', index_cols='Date')
    total_15_funds_weight_0 = get_sql(level4_factor_result, 'total_15_funds_weight_0', index_cols='Date')
    total_15_funds_weight_1 = get_sql(level4_factor_result, 'total_15_funds_weight_1', index_cols='Date')
    total_15_funds_weight_2 = get_sql(level4_factor_result, 'total_15_funds_weight_2', index_cols='Date')
    total_15_funds_weight_3 = get_sql(level4_factor_result, 'total_15_funds_weight_3', index_cols='Date')
    total_15_funds_weight_4 = get_sql(level4_factor_result, 'total_15_funds_weight_4', index_cols='Date')
    total_15_funds_weight_5 = get_sql(level4_factor_result, 'total_15_funds_weight_5', index_cols='Date')
    total_30_funds_weight_0 = get_sql(level4_factor_result, 'total_30_funds_weight_0', index_cols='Date')
    total_30_funds_weight_1 = get_sql(level4_factor_result, 'total_30_funds_weight_1', index_cols='Date')
    total_30_funds_weight_2 = get_sql(level4_factor_result, 'total_30_funds_weight_2', index_cols='Date')
    total_30_funds_weight_3 = get_sql(level4_factor_result, 'total_30_funds_weight_3', index_cols='Date')
    total_30_funds_weight_4 = get_sql(level4_factor_result, 'total_30_funds_weight_4', index_cols='Date')
    total_30_funds_weight_5 = get_sql(level4_factor_result, 'total_30_funds_weight_5', index_cols='Date')

    return top_funds_return, [total_9_funds_weight_0, total_10_funds_weight_0, total_15_funds_weight_0, total_30_funds_weight_0], \
    [total_9_funds_weight_1, total_10_funds_weight_1, total_15_funds_weight_1, total_30_funds_weight_1], \
    [total_9_funds_weight_2, total_10_funds_weight_2, total_15_funds_weight_2, total_30_funds_weight_2], \
    [total_9_funds_weight_3, total_10_funds_weight_3, total_15_funds_weight_3, total_30_funds_weight_3], \
    [total_9_funds_weight_4, total_10_funds_weight_4, total_15_funds_weight_4, total_30_funds_weight_4], \
    [total_9_funds_weight_5, total_10_funds_weight_5, total_15_funds_weight_5, total_30_funds_weight_5]


def handle(top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme):
    
    types_name = ['most_conservative', 'moderate_conservative', 'balanced', 'moderate_risky', 'most_risky', 'extreme']
    all_types = [most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme]
    all_tables = []
    for rank, one_type in enumerate(all_types):
        all_num_return = []
        for num in one_type:
            co_time = np.intersect1d(top_funds_return.index, num.index)
            num_return = top_funds_return.loc[co_time, num.columns]
            num_return = (num_return * num).sum(axis=1)
            all_num_return.append(num_return)
        all_num_return = pd.concat(all_num_return, axis=1)
        all_num_return.columns = [f'total_9_{types_name[rank]}',
            f'total_10_{types_name[rank]}',
            f'total_15_{types_name[rank]}',
            f'total_30_{types_name[rank]}']

        all_tables.append(all_num_return)
    
    return all_tables

def upload(all_tables):
    all_tables[0].to_sql('funds_return_0', con=level4_factor_result, if_exists='replace')
    all_tables[1].to_sql('funds_return_1', con=level4_factor_result, if_exists='replace')
    all_tables[2].to_sql('funds_return_2', con=level4_factor_result, if_exists='replace')
    all_tables[3].to_sql('funds_return_3', con=level4_factor_result, if_exists='replace')
    all_tables[4].to_sql('funds_return_4', con=level4_factor_result, if_exists='replace')
    all_tables[5].to_sql('funds_return_5', con=level4_factor_result, if_exists='replace')

@timeit('level4/portfolio_return')
def portfolio_return():
    top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme = read_sql()
    all_tables = handle(top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme)
    upload(all_tables)
