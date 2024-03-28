from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level0_joinquant, level1_csmar, level2_csmar, level3_factors
from utils.frequent_dates import next_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    Fund_Portfolio_Stock = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    hs300_component = get_sql(level1_csmar, 'hs300_component')
    sz50_component = get_sql(level1_csmar, 'sz50_component')
    zz500_component = get_sql(level1_csmar, 'zz500_component')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])

    return Fund_Portfolio_Stock, Classification, hs300_component, sz50_component, zz500_component, Fund_Return_m

def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def handle(Fund_Portfolio_Stock, Classification, hs300_component, sz50_component, zz500_component, Fund_Return_m):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)
    Fund_Portfolio_Stock = Fund_Portfolio_Stock[Fund_Portfolio_Stock.Symbol.isin(filtered_risky_symbol)]
    hs300_component = hs300_component.drop_duplicates().pivot(index='Date', columns='Stkcd', values='Weight').resample('d').ffill()
    hs300_component = hs300_component.stack()
    hs300_component.name = 'hs300'
    hs300_component = hs300_component.reset_index()

    sz50_component = sz50_component.drop_duplicates().pivot(index='Date', columns='Stkcd', values='Weight').resample('d').ffill()
    sz50_component = sz50_component.stack()
    sz50_component.name = 'sz50'
    sz50_component = sz50_component.reset_index()

    zz500_component = zz500_component.drop_duplicates().pivot(index='Date', columns='Stkcd', values='Weight').resample('d').ffill()
    zz500_component = zz500_component.stack()
    zz500_component.name = 'zz500'
    zz500_component = zz500_component.reset_index()

    compare_proportion = pd.merge(Fund_Portfolio_Stock, hs300_component, on=['Date', 'Stkcd'], how='left').fillna(0)
    compare_proportion = pd.merge(compare_proportion, sz50_component, on=['Date', 'Stkcd'], how='left').fillna(0)
    compare_proportion = pd.merge(compare_proportion, zz500_component, on=['Date', 'Stkcd'], how='left').fillna(0)
    compare_proportion['hs300_abs'] = np.abs(compare_proportion.Proportion - compare_proportion.hs300)
    compare_proportion['sz50_abs'] = np.abs(compare_proportion.Proportion - compare_proportion.sz50)
    compare_proportion['zz500_abs'] = np.abs(compare_proportion.Proportion - compare_proportion.zz500)
    compare_proportion['min_abs'] = compare_proportion[['hs300_abs', 'sz50_abs', 'zz500_abs']].apply(min, axis=1)
    active_share = compare_proportion.groupby(['Date', 'Symbol']).apply(lambda x: x.min_abs.sum()) / (100 * 2)

    co_index = np.intersect1d(active_share.index, Fund_Return_m.index)
    active_share = active_share.loc[co_index]

    active_share.loc[next_month_end] = np.nan
    active_share = active_share.unstack().resample('M').ffill().stack()

    active_share.name = 'active_share'
    return active_share

def upload(active_share):
    active_share.to_sql('active_share', con=level3_factors, if_exists='replace')

@timeit('level3/active_share')
def active_share():
    Fund_Portfolio_Stock, Classification, hs300_component, sz50_component, zz500_component, Fund_Return_m = read_sql()
    active_share = handle(Fund_Portfolio_Stock, Classification, hs300_component, sz50_component, zz500_component, Fund_Return_m)
    upload(active_share)
