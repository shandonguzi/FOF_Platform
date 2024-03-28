
from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level0_joinquant, level1_csmar, level2_csmar, level3_factors
from utils.frequent_dates import next_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Fund_Portfolio_Stock = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')

    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m')
    Fund_FeesChange = get_sql(level1_csmar, 'Fund_FeesChange')
    TRD_Mnth = get_sql(level1_csmar, 'TRD_Mnth', index_cols=['Date', 'Stkcd']).RealPctChange

    return Fund_Portfolio_Stock, Classification, Fund_Return_m, Fund_FeesChange, TRD_Mnth

def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def handle(Fund_Portfolio_Stock, Classification, Fund_Return_m, Fund_FeesChange, TRD_Mnth):
    filtered_risky_symbol = get_filtered_risky_symbol(Classification)
    Fund_Portfolio_Stock = Fund_Portfolio_Stock[Fund_Portfolio_Stock.Symbol.isin(filtered_risky_symbol)]
    Fund_Portfolio_Stock = Fund_Portfolio_Stock[~ Fund_Portfolio_Stock[['Date', 'Symbol', 'Stkcd']].duplicated()]
    Fund_Portfolio_Stock_proportion = Fund_Portfolio_Stock.groupby('Symbol').apply(lambda x: x.pivot(index='Date', columns='Stkcd', values='Proportion').resample('M').ffill().stack())
    Fund_Portfolio_Stock_proportion.name = 'Proportion'
    TRD_Mnth.index = TRD_Mnth.index.set_levels(TRD_Mnth.index.levels[0] + pd.offsets.MonthEnd(0), level=0)
    Fund_Portfolio_Stock_m = pd.merge(Fund_Portfolio_Stock_proportion.reset_index(), TRD_Mnth, on=['Date', 'Stkcd'], how='left').dropna()
    Fund_Portfolio_Stock_m['Proportion'] /= 100
    Fund_Portfolio_Stock_m['WeightedRealPctChange'] = Fund_Portfolio_Stock_m.Proportion * Fund_Portfolio_Stock_m.RealPctChange
    Fund_Portfolio_Stock_m = Fund_Portfolio_Stock_m.groupby(['Date', 'Symbol']).apply(lambda x: x.WeightedRealPctChange.sum())
    Fund_Portfolio_Stock_m.name = 'WeightedRealPctChangeSum'
    compare = pd.merge(Fund_Portfolio_Stock_m, Fund_Return_m, on=['Date', 'Symbol'], how='left').dropna()
    fees = Fund_FeesChange[Fund_FeesChange.NameOfFee.isin(['管理费率', '托管费率'])].groupby(['Date', 'Symbol']).apply(lambda x: x.ProportionOfFee.sum())
    fees = fees.unstack().resample('M').last().ffill().stack()
    fees.name = 'fees'
    final_df = pd.merge(compare, fees / 100, on=['Date', 'Symbol'], how='left').dropna()
    final_df['RG'] = final_df.Fund_Return_m - (final_df.WeightedRealPctChangeSum - final_df.fees)
    rg = final_df.set_index(['Date', 'Symbol']).RG

    rg.loc[next_month_end] = np.nan
    rg = rg.unstack().resample('M').ffill().stack()
    rg = rg.rolling(36, 24).mean()
    rg.name = 'return_gap'

    return rg

def upload(rg):
    rg.to_sql('return_gap', con=level3_factors, if_exists='replace')

@timeit('level3/return_gap')
def return_gap():
    Fund_Portfolio_Stock, Classification, Fund_Return_m, Fund_FeesChange, TRD_Mnth = read_sql()
    rg = handle(Fund_Portfolio_Stock, Classification, Fund_Return_m, Fund_FeesChange, TRD_Mnth)
    upload(rg)
