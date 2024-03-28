
import numpy as np
import pandas as pd

from settings.database import (errata_jiayin_robo_advisor,
                               product_jiayin_robo_advisor)
from utils.frequent_dates import next_month_begin_s, this_month_begin_s, last_month_begin
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    old_top_funds_return = get_sql(product_jiayin_robo_advisor, 'top_funds_return', index_cols='Date')
    new_top_funds_return = get_sql(errata_jiayin_robo_advisor, 'top_funds_return', index_cols='Date')
    total_10_funds_weight_0 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_0', index_cols='Date')
    total_10_funds_weight_1 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_1', index_cols='Date')
    total_10_funds_weight_2 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_2', index_cols='Date')
    total_10_funds_weight_3 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_3', index_cols='Date')
    total_10_funds_weight_4 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_4', index_cols='Date')
    # total_10_funds_weight_5 = get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_5', index_cols='Date')
    total_9_funds_weight_0 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_0', index_cols='Date')
    total_9_funds_weight_1 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_1', index_cols='Date')
    total_9_funds_weight_2 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_2', index_cols='Date')
    total_9_funds_weight_3 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_3', index_cols='Date')
    total_9_funds_weight_4 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_4', index_cols='Date')
    # total_9_funds_weight_5 = get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_5', index_cols='Date')
    total_15_funds_weight_0 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_0', index_cols='Date')
    total_15_funds_weight_1 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_1', index_cols='Date')
    total_15_funds_weight_2 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_2', index_cols='Date')
    total_15_funds_weight_3 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_3', index_cols='Date')
    total_15_funds_weight_4 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_4', index_cols='Date')
    # total_15_funds_weight_5 = get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_5', index_cols='Date')
    total_30_funds_weight_0 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_0', index_cols='Date')
    total_30_funds_weight_1 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_1', index_cols='Date')
    total_30_funds_weight_2 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_2', index_cols='Date')
    total_30_funds_weight_3 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_3', index_cols='Date')
    total_30_funds_weight_4 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_4', index_cols='Date')
    # total_30_funds_weight_5 = get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_5', index_cols='Date')

    return old_top_funds_return, new_top_funds_return, [total_9_funds_weight_0, total_10_funds_weight_0, total_15_funds_weight_0, total_30_funds_weight_0], \
    [total_9_funds_weight_1, total_10_funds_weight_1, total_15_funds_weight_1, total_30_funds_weight_1], \
    [total_9_funds_weight_2, total_10_funds_weight_2, total_15_funds_weight_2, total_30_funds_weight_2], \
    [total_9_funds_weight_3, total_10_funds_weight_3, total_15_funds_weight_3, total_30_funds_weight_3], \
    [total_9_funds_weight_4, total_10_funds_weight_4, total_15_funds_weight_4, total_30_funds_weight_4]


def handle(old_top_funds_return, new_top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky):
    top_funds_return = old_top_funds_return.copy()
    top_funds_return.loc[last_month_begin] = new_top_funds_return.loc[last_month_begin]

    # try:
    #     top_funds_return.loc[next_month_begin_s] = new_top_funds_return.loc[next_month_begin_s]
    # except:
    #     try:
    #         top_funds_return.loc[this_month_begin_s] = new_top_funds_return.loc[this_month_begin_s]
    #     except:
    #         # 月初是非交易日时，在月初将取不到当月最后一天基金的复权净值，在这种情况下取当月的值
    #         top_funds_return.loc[this_month_begin_s] = new_top_funds_return.loc[this_month_begin_s]

    top_funds_return.index = pd.to_datetime(top_funds_return.index)
    types_name = ['most_conservative', 'moderate_conservative', 'balanced', 'moderate_risky', 'most_risky', 'extreme']
    all_types = [most_conservative, moderate_conservative, balanced, moderate_risky, most_risky]
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
    all_tables[0].to_sql('funds_return_0', con=errata_jiayin_robo_advisor, if_exists='replace')
    all_tables[1].to_sql('funds_return_1', con=errata_jiayin_robo_advisor, if_exists='replace')
    all_tables[2].to_sql('funds_return_2', con=errata_jiayin_robo_advisor, if_exists='replace')
    all_tables[3].to_sql('funds_return_3', con=errata_jiayin_robo_advisor, if_exists='replace')
    all_tables[4].to_sql('funds_return_4', con=errata_jiayin_robo_advisor, if_exists='replace')
    # all_tables[5].to_sql('funds_return_5', con=errata_jiayin_robo_advisor, if_exists='replace')

@timeit('errata/portfolio_return')
def portfolio_return():
    old_top_funds_return, new_top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky = read_sql()
    all_tables = handle(old_top_funds_return, new_top_funds_return, most_conservative, moderate_conservative, balanced, moderate_risky, most_risky)
    upload(all_tables)
