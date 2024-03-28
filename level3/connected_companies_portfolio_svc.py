
from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level0_joinquant, level1_csmar, level3_factors
from utils.frequent_dates import next_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Fund_Portfolio_Stock = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    svc_α = get_sql(level3_factors, 'svc_α', index_cols=['Date', 'Symbol'])

    return Fund_Portfolio_Stock, Classification, svc_α

def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def handle(Fund_Portfolio_Stock, Classification, svc_α):


    filtered_risky_symbol = get_filtered_risky_symbol(Classification)

    Fund_Portfolio_Stock = Fund_Portfolio_Stock[Fund_Portfolio_Stock.Symbol.isin(filtered_risky_symbol)]
    Fund_Portfolio_Stock = Fund_Portfolio_Stock[~ Fund_Portfolio_Stock[['Date', 'Symbol', 'Stkcd']].duplicated()]

    v_m_n = Fund_Portfolio_Stock.groupby(['Date', 'Stkcd']).apply(lambda x: x.Proportion / x.Proportion.sum())
    v_m_n = v_m_n.sort_index(level=2)
    Fund_Portfolio_Stock['v_m_n'] = v_m_n.values

    svc_α.index = svc_α.index.set_levels(svc_α.index.levels[0] + pd.offsets.MonthEnd(0), level=0)
    Fund_Portfolio_Stock = pd.merge(Fund_Portfolio_Stock[['Date', 'Symbol', 'Stkcd', 'Proportion', 'v_m_n']], svc_α.reset_index(), on=['Date', 'Symbol'])
    Fund_Portfolio_Stock['δ'] = Fund_Portfolio_Stock.v_m_n * Fund_Portfolio_Stock.svc_α
    δ_n_t = Fund_Portfolio_Stock.groupby(['Date', 'Stkcd']).apply(lambda x: x.δ.sum())
    δ_n_t.name = 'δ_n_t'

    Fund_Portfolio_Stock = pd.merge(Fund_Portfolio_Stock, δ_n_t.reset_index(), on=['Date', 'Stkcd'])

    Fund_Portfolio_Stock['ccp'] = Fund_Portfolio_Stock.Proportion * Fund_Portfolio_Stock.δ

    ccp = Fund_Portfolio_Stock.groupby(['Date', 'Symbol']).apply(lambda x: (x.δ_n_t * x.Proportion).sum())

    ccp.loc[next_month_end] = np.nan
    ccp = ccp.unstack().resample('M').ffill().stack()

    ccp.name = 'connected_companies_portfolio_svc'
    
    return ccp

def upload(ccp):
    ccp.to_sql('connected_companies_portfolio_svc', con=level3_factors, if_exists='replace')

@timeit('level3/connected_companies_portfolio_svc')
def connected_companies_portfolio_svc():
    Fund_Portfolio_Stock, Classification, svc_α = read_sql()
    ccp = handle(Fund_Portfolio_Stock, Classification, svc_α)
    upload(ccp)
