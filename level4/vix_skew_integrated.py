
import pandas as pd
import numpy as np

from settings.database import level3_factors, level4_factor_result
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from utils.frequent_dates import next_month_begin, this_month_begin_s, this_month_begin


def read_sql():

    VIX_SKEW = get_sql(level3_factors, 'VIX_SKEW', index_cols='Date')

    total_9_funds_weight_2 = get_sql(level4_factor_result, 'total_9_funds_weight_2', index_cols='Date')
    total_9_funds_weight_3 = get_sql(level4_factor_result, 'total_9_funds_weight_3', index_cols='Date')
    total_9_funds_weight_4 = get_sql(level4_factor_result, 'total_9_funds_weight_4', index_cols='Date')
    
    total_10_funds_weight_2 = get_sql(level4_factor_result, 'total_10_funds_weight_2', index_cols='Date')
    total_10_funds_weight_3 = get_sql(level4_factor_result, 'total_10_funds_weight_3', index_cols='Date')
    total_10_funds_weight_4 = get_sql(level4_factor_result, 'total_10_funds_weight_4', index_cols='Date')

    total_15_funds_weight_2 = get_sql(level4_factor_result, 'total_15_funds_weight_2', index_cols='Date')
    total_15_funds_weight_3 = get_sql(level4_factor_result, 'total_15_funds_weight_3', index_cols='Date')
    total_15_funds_weight_4 = get_sql(level4_factor_result, 'total_15_funds_weight_4', index_cols='Date')

    total_30_funds_weight_2 = get_sql(level4_factor_result, 'total_30_funds_weight_2', index_cols='Date')
    total_30_funds_weight_3 = get_sql(level4_factor_result, 'total_30_funds_weight_3', index_cols='Date')
    total_30_funds_weight_4 = get_sql(level4_factor_result, 'total_30_funds_weight_4', index_cols='Date')

    total_9 = [total_9_funds_weight_2, total_9_funds_weight_3, total_9_funds_weight_4]
    total_10 = [total_10_funds_weight_2, total_10_funds_weight_3, total_10_funds_weight_4]
    total_15 = [total_15_funds_weight_2, total_15_funds_weight_3, total_15_funds_weight_4]
    total_30 = [total_30_funds_weight_2, total_30_funds_weight_3, total_30_funds_weight_4]

    return VIX_SKEW, total_9, total_10, total_15, total_30

def get_rebalance_weight(raw_weight, VIX_SKEW):
    raw_weight_extended = raw_weight.copy()
    raw_weight_extended.loc[next_month_begin] = raw_weight.loc[this_month_begin_s]
    risky_raw_weight_daily = raw_weight_extended.loc['2020': , raw_weight_extended.columns.str.contains('Risky')].resample('d').ffill()
    previous_risky_raw_weight_sum = risky_raw_weight_daily.sum(axis=1)
    VIX_SKEW_extended = VIX_SKEW.copy()
    VIX_SKEW_extended.loc[next_month_begin] = VIX_SKEW_extended.iloc[-1]
    VIX_SKEW = VIX_SKEW_extended.resample('d').ffill()
    risky_raw_weight_daily = pd.merge(risky_raw_weight_daily, VIX_SKEW.vol_and_skew_weight, on='Date')
    risky_weight_daily = risky_raw_weight_daily.iloc[:, :-1].apply(lambda fund: fund * risky_raw_weight_daily.iloc[:, -1] * 2)

    changed_part = previous_risky_raw_weight_sum.loc[risky_weight_daily.index] - risky_weight_daily.sum(axis=1)

    not_risky_raw_weight_daily = raw_weight_extended.loc['2020': , ~ raw_weight_extended.columns.str.contains('Risky')].resample('d').ffill()

    not_risky_multiple = (changed_part / not_risky_raw_weight_daily.sum(axis=1)).dropna()

    not_risky_raw_weight_daily = not_risky_raw_weight_daily.apply(lambda fund: fund * (1 + not_risky_multiple))

    risky_raw_weight = raw_weight.loc['2020-2': , raw_weight.columns.str.contains('Risky')]
    try:
        risky_weight_daily.loc[risky_raw_weight.index] = risky_raw_weight.copy()
    except:
        risky_weight_daily.loc[risky_raw_weight.drop(next_month_begin).index] = risky_raw_weight.copy()

    rebalance_weight = pd.concat([risky_weight_daily, not_risky_raw_weight_daily], axis=1).ffill().dropna()
    advise_on_rebalance = np.abs(changed_part) > .10
    advise_on_entry = VIX_SKEW.vol_and_skew_weight > .55

    return rebalance_weight, advise_on_rebalance, advise_on_entry


def handle(VIX_SKEW, total_9, total_10, total_15, total_30):
    
    VIX_SKEW.loc[this_month_begin] = VIX_SKEW.iloc[-1].values
    VIX_SKEW.index = pd.to_datetime(VIX_SKEW.index)
    VIX_SKEW = VIX_SKEW.resample('d').ffill()
    total_9_funds_weight_2_rebalance, _, _ = get_rebalance_weight(total_9[0], VIX_SKEW)
    total_9_funds_weight_3_rebalance, _, _ = get_rebalance_weight(total_9[1], VIX_SKEW)
    total_9_funds_weight_4_rebalance, _, _ = get_rebalance_weight(total_9[2], VIX_SKEW)

    total_10_funds_weight_2_rebalance, _, _ = get_rebalance_weight(total_10[0], VIX_SKEW)
    total_10_funds_weight_3_rebalance, _, _ = get_rebalance_weight(total_10[1], VIX_SKEW)
    total_10_funds_weight_4_rebalance, _, _ = get_rebalance_weight(total_10[2], VIX_SKEW)

    total_15_funds_weight_2_rebalance, advise_on_rebalance_2, advise_on_entry_2 = get_rebalance_weight(total_15[0], VIX_SKEW)
    total_15_funds_weight_3_rebalance, advise_on_rebalance_3, advise_on_entry_3 = get_rebalance_weight(total_15[1], VIX_SKEW)
    total_15_funds_weight_4_rebalance, advise_on_rebalance_4, advise_on_entry_4 = get_rebalance_weight(total_15[2], VIX_SKEW)

    total_30_funds_weight_2_rebalance, _, _ = get_rebalance_weight(total_30[0], VIX_SKEW)
    total_30_funds_weight_3_rebalance, _, _ = get_rebalance_weight(total_30[1], VIX_SKEW)
    total_30_funds_weight_4_rebalance, _, _ = get_rebalance_weight(total_30[2], VIX_SKEW)


    advise_on_rebalance = pd.concat([advise_on_rebalance_2, advise_on_rebalance_3, advise_on_rebalance_4], axis=1)
    advise_on_rebalance.columns = ['Portfolio_2', 'Portfolio_3', 'Portfolio_4']

    advise_on_entry = pd.concat([advise_on_entry_2, advise_on_entry_3, advise_on_entry_4], axis=1)
    advise_on_entry.columns = ['Portfolio_2', 'Portfolio_3', 'Portfolio_4']

    total_9 = [total_9_funds_weight_2_rebalance, total_9_funds_weight_3_rebalance, total_9_funds_weight_4_rebalance]
    total_10 = [total_10_funds_weight_2_rebalance, total_10_funds_weight_3_rebalance, total_10_funds_weight_4_rebalance]
    total_15 = [total_15_funds_weight_2_rebalance, total_15_funds_weight_3_rebalance, total_15_funds_weight_4_rebalance]
    total_30 = [total_30_funds_weight_2_rebalance, total_30_funds_weight_3_rebalance, total_30_funds_weight_4_rebalance]

    return total_9, total_10, total_15, total_30, advise_on_rebalance, advise_on_entry

def upload(total_9, total_10, total_15, total_30, advise_on_rebalance, advise_on_entry):

    total_9[0].to_sql('total_9_funds_weight_2_rebalance', con=level4_factor_result, if_exists='replace')
    total_9[1].to_sql('total_9_funds_weight_3_rebalance', con=level4_factor_result, if_exists='replace')
    total_9[2].to_sql('total_9_funds_weight_4_rebalance', con=level4_factor_result, if_exists='replace')

    total_10[0].to_sql('total_10_funds_weight_2_rebalance', con=level4_factor_result, if_exists='replace')
    total_10[1].to_sql('total_10_funds_weight_3_rebalance', con=level4_factor_result, if_exists='replace')
    total_10[2].to_sql('total_10_funds_weight_4_rebalance', con=level4_factor_result, if_exists='replace')

    total_15[0].to_sql('total_15_funds_weight_2_rebalance', con=level4_factor_result, if_exists='replace')
    total_15[1].to_sql('total_15_funds_weight_3_rebalance', con=level4_factor_result, if_exists='replace')
    total_15[2].to_sql('total_15_funds_weight_4_rebalance', con=level4_factor_result, if_exists='replace')

    total_30[0].to_sql('total_30_funds_weight_2_rebalance', con=level4_factor_result, if_exists='replace')
    total_30[1].to_sql('total_30_funds_weight_3_rebalance', con=level4_factor_result, if_exists='replace')
    total_30[2].to_sql('total_30_funds_weight_4_rebalance', con=level4_factor_result, if_exists='replace')

    advise_on_rebalance.to_sql('advise_on_rebalance', con=level4_factor_result, if_exists='replace')
    advise_on_entry.to_sql('advise_on_entry', con=level4_factor_result, if_exists='replace')


@timeit('level4/vix_skew_integrated')
def vix_skew_integrated():
    VIX_SKEW, total_9, total_10, total_15, total_30 = read_sql()
    total_9, total_10, total_15, total_30, advise_on_rebalance, advise_on_entry = handle(VIX_SKEW, total_9, total_10, total_15, total_30)
    upload(total_9, total_10, total_15, total_30, advise_on_rebalance, advise_on_entry)
