from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level0_joinquant, level1_csmar, level2_csmar, level3_factors
from utils.frequent_dates import next_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    Fund_Portfolio_Stock = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    jq_stock_classification = get_sql(level0_joinquant, 'Stock_SWL1_Clsf')
    hs300_component = get_sql(level1_csmar, 'hs300_component')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])
    return Fund_Portfolio_Stock, jq_stock_classification, hs300_component, Classification, Fund_Return_m

def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def handle(Fund_Portfolio_Stock, jq_stock_classification, hs300_component, Classification, Fund_Return_m):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)
    Fund_Portfolio_Stock = Fund_Portfolio_Stock[Fund_Portfolio_Stock.Symbol.isin(filtered_risky_symbol)]

    Fund_Portfolio_proportion = pd.merge(Fund_Portfolio_Stock, jq_stock_classification, left_on='Stkcd', right_on='Stkcd', how='left').groupby(['Date', 'Symbol', 'Indnme']).Proportion.sum().sort_index()
    hs300_proportion = pd.merge(hs300_component, jq_stock_classification, left_on='Stkcd', right_on='Stkcd', how='left').groupby(['Date', 'Indnme']).Weight.sum().sort_index()

    hs300_proportion = hs300_proportion.unstack().resample('d').ffill().stack()
    hs300_proportion.name = 'hs300_weight'

    Fund_Portfolio_proportion_comparision = pd.merge(hs300_proportion.reset_index(), Fund_Portfolio_proportion.reset_index(), on=['Date', 'Indnme'], how='right').set_index(['Symbol', 'Date', 'Indnme']).dropna()
    two_demi_index = Fund_Portfolio_proportion_comparision.reset_index().set_index(['Date', 'Symbol']).index
    co_index = np.intersect1d(two_demi_index, Fund_Return_m.index)
    Fund_Portfolio_proportion_comparision = Fund_Portfolio_proportion_comparision.reset_index().set_index(['Date', 'Symbol']).loc[co_index]
    Fund_Portfolio_proportion_comparision = Fund_Portfolio_proportion_comparision.set_index(['Indnme'], append=True)

    industry_concentration = Fund_Portfolio_proportion_comparision.groupby(level=[0, 1]).apply(lambda x: ((x.hs300_weight - x.Proportion) ** 2).sum())

    industry_concentration = industry_concentration.sort_index().unstack().resample('m').last().ffill().stack()
    industry_concentration.loc[next_month_end] = np.nan
    industry_concentration = industry_concentration.unstack().resample('M').ffill().stack()
    industry_concentration.name = 'industry_concentration'

    return industry_concentration


def upload(industry_concentration):
    industry_concentration.to_sql('industry_concentration', con=level3_factors, if_exists='replace')

@timeit('level3/industry_concentration')
def industry_concentration():
    Fund_Portfolio_Stock, jq_stock_classification, hs300_component, Classification, Fund_Return_m = read_sql()
    industry_concentration = handle(Fund_Portfolio_Stock, jq_stock_classification, hs300_component, Classification, Fund_Return_m)
    upload(industry_concentration)
