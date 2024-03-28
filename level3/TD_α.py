
import numpy as np
import pandas as pd
from parallel_pandas import ParallelPandas
from sklearn.linear_model import LinearRegression

from settings.database import level1_wind, level2_csmar, level3_factors
from utils.factor_research.truncate_funcs import truncate_factor
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

ParallelPandas.initialize(8)

def read_sql():

    Interest_Bond = get_sql(level2_csmar, 'Interest_Bond')
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])
    bond_term = get_sql(level3_factors, 'bond_term', index_cols='Date')
    bond_default = get_sql(level3_factors, 'bond_default', index_cols='Date')
    TreasuryBond1M = get_sql(level1_wind, 'TreasuryBond1M', index_cols='Date')

    return Interest_Bond, Fund_Return_m, bond_term, bond_default, TreasuryBond1M

def TD_α_generation(months_return, regression_source):
    months_return = months_return.dropna()
    temp_dates = months_return.index
    temp_regression = regression_source.loc[temp_dates]
    α = LinearRegression().fit(X = temp_regression[['DEF', 'TERM']].values, y = (months_return - temp_regression.rf).values).intercept_
    return α

def handle(Interest_Bond, Fund_Return_m, bond_term, bond_default, TreasuryBond1M):

    co_index = np.intersect1d(Fund_Return_m.index, pd.MultiIndex.from_frame(Interest_Bond[['Date', 'Symbol']]))
    RETURN_MIN_NOT_NA_NUM = 36
    m_return_Interest_Bond = Fund_Return_m.loc[co_index].unstack()
    m_return_Interest_Bond = m_return_Interest_Bond.loc[:, pd.notna(m_return_Interest_Bond).sum() > RETURN_MIN_NOT_NA_NUM]
    regression_source = pd.concat([bond_term, bond_default, TreasuryBond1M], axis=1).dropna()
    regression_source.columns = ['TERM', 'DEF', 'rf']
    regression_source.index.name = 'Date'

    ROLLING_WINDOW = 36
    REGRESSION_MAX_NA_NUM = 24

    regression_time = np.intersect1d(regression_source.index, m_return_Interest_Bond.index)
    m_return_Interest_Bond, regression_source = m_return_Interest_Bond.loc[regression_time], regression_source.loc[regression_time]
    TD_α_matrix = m_return_Interest_Bond.rolling(window = ROLLING_WINDOW, min_periods = ROLLING_WINDOW - REGRESSION_MAX_NA_NUM).p_apply(lambda months_return: TD_α_generation(months_return, regression_source))
    TD_α_matrix = TD_α_matrix.dropna(how='all')
    TD_α_matrix.index = TD_α_matrix.index + pd.offsets.MonthBegin(0)
    TD_α = TD_α_matrix.stack().loc[truncate_factor(TD_α_matrix, m_return_Interest_Bond, simple_truncate_proportion=.03, ivol_truncate_proportion=.03, value_threshold=1e7, return_truncate_proportion=.03)]
    
    TD_α.name = 'TD_α'
    return TD_α

def upload(TD_α):
    TD_α.to_sql('TD_α', con=level3_factors, if_exists='replace')

@timeit('level3/TD_α')
def TD_α():
    Interest_Bond, Fund_Return_m, bond_term, bond_default, TreasuryBond1M = read_sql()
    TD_α = handle(Interest_Bond, Fund_Return_m, bond_term, bond_default, TreasuryBond1M)
    upload(TD_α)
