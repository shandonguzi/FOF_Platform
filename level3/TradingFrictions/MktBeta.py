
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level0_jiayin, level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from parallel_pandas import ParallelPandas
from sklearn.linear_model import LinearRegression

ParallelPandas.initialize(8, disable_pr_bar=True)

def read_sql():

    Stock_Return_m = get_sql(level1_csmar, 'TRD_Mnth')
    Stock_Return_this_m = get_sql(level2_csmar, 'cm_return')
    CH3 = get_sql(level0_jiayin, 'm_CH3', index_cols='Date')
    
    return Stock_Return_m, Stock_Return_this_m, CH3


def capm_𝛽_generation(months_return, CH3):

    months_return = months_return.dropna()
    temp_dates = months_return.index
    temp_regression = CH3[['mktrf', 'rf', 'smb', 'vmg']].loc[temp_dates]
    𝛽 = LinearRegression().fit(X = temp_regression.mktrf.values.reshape(-1, 1), y = (months_return - temp_regression.rf).values).coef_[0]
    return 𝛽


def handle(Stock_Return_m, Stock_Return_this_m, CH3):

    # Stock_Return_m = Stock_Return_m[['Stkcd', 'Date', 'RealPctChange']]
    # Stock_Return_this_m = Stock_Return_this_m[['Stkcd', 'Date', 'RealPctChange']]
    # Stock_Return_m = pd.concat([Stock_Return_m, Stock_Return_this_m], axis=0).reset_index(drop=True).sort_values(['Date', 'Stkcd']).set_index(['Date', 'Stkcd'])
    Stock_Return_m = Stock_Return_m[['Stkcd', 'Date', 'RealPctChange']].sort_values(['Date', 'Stkcd']).set_index(['Date', 'Stkcd'])

    regression_m_return = Stock_Return_m.unstack()
    regression_m_return = regression_m_return.loc[:, pd.notna(regression_m_return).sum() >= 24]
    regression_m_return = regression_m_return.loc['1991-12-01':]
    CH3.index = CH3.index - pd.offsets.MonthBegin(1)

    mkt_beta = regression_m_return.rolling(window = 60, min_periods = 24).p_apply(lambda months_return: capm_𝛽_generation(months_return, CH3))
    mkt_beta = mkt_beta.stack()
    mkt_beta = mkt_beta.reset_index().rename(columns={'RealPctChange': 'MktBeta'})

    return mkt_beta


def upload(mkt_beta):
    mkt_beta.to_sql('MktBeta', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/MktBeta')
def MktBeta():
    Stock_Return_m, Stock_Return_this_m, CH3 = read_sql()
    mkt_beta = handle(Stock_Return_m, Stock_Return_this_m, CH3)
    upload(mkt_beta)