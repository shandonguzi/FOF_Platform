
from functools import reduce

import numpy as np
import pandas as pd
from parallel_pandas import ParallelPandas
from sklearn.linear_model import LinearRegression

from settings.database import level0_jiayin, level0_joinquant, level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

ParallelPandas.initialize(8)

def read_sql():

    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])
    CH3 = get_sql(level0_jiayin, 'm_CH3', index_cols='Date')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    
    return Fund_Return_m, CH3, Classification


def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def get_Fund_Allocation_filter(filtered_risky_symbol):

    Fund_Allocation = get_sql(level1_csmar, f'select * from Fund_Allocation_m where EquityProportion > 0.3 and Symbol in {tuple(filtered_risky_symbol)}')
    Fund_Allocation_filter = pd.MultiIndex.from_frame(Fund_Allocation[['Date', 'Symbol']])

    return Fund_Allocation_filter


def capm_α_generation(months_return, CH3):

    months_return = months_return.dropna()
    temp_dates = months_return.index
    temp_regression = CH3[['mktrf', 'rf', 'smb', 'vmg']].loc[temp_dates]
    α = LinearRegression().fit(X = temp_regression.mktrf.values.reshape(-1, 1), y = (months_return - temp_regression.rf).values).intercept_
    return α


def handle(Fund_Return_m, CH3, Classification):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)
    Fund_Allocation_filter = get_Fund_Allocation_filter(filtered_risky_symbol)

    m_return_of_risky_fund = Fund_Return_m.copy()

    filtered_m_return_of_risky_fund = m_return_of_risky_fund.loc[np.intersect1d(m_return_of_risky_fund.index, Fund_Allocation_filter)]

    regression_m_return_of_risky_fund = filtered_m_return_of_risky_fund.unstack()
    regression_m_return_of_risky_fund = regression_m_return_of_risky_fund.loc[:, pd.notna(regression_m_return_of_risky_fund).sum() > 12]

    MAX_NA_NUM = 30

    capm_α = regression_m_return_of_risky_fund.rolling(window = 36, min_periods = 36 - MAX_NA_NUM).p_apply(lambda months_return: capm_α_generation(months_return, CH3))
    capm_α.index = capm_α.index + pd.offsets.MonthBegin(0)
    capm_α = capm_α.stack()
    m_return_of_risky_fund_index = m_return_of_risky_fund.index.set_levels(m_return_of_risky_fund.index.levels[0] + pd.offsets.MonthBegin(0), level=0)
    co_time = np.intersect1d(m_return_of_risky_fund_index, capm_α.index)
    capm_α = capm_α.loc[co_time]

    capm_α.name = 'capm_α'

    return capm_α


def upload(capm_α):
    capm_α.to_sql('capm_α', con=level3_factors, if_exists='replace')


@timeit('level3/capm_α')
def capm_α():
    Fund_Return_m, CH3, Classification = read_sql()
    capm_α = handle(Fund_Return_m, CH3, Classification)
    upload(capm_α)
