

import numpy as np
import pandas as pd
import statsmodels.api as sm
from settings.database import level1_wind

from utils.mysql.get_sql import get_sql


def get_annual_sharpe(portfolio_return):
    risk_free = get_sql(level1_wind, 'TreasuryBond1M', index_cols='Date')
    risk_free.index = risk_free.index + pd.offsets.MonthBegin(-1)
    co_time = np.intersect1d(portfolio_return.index, risk_free.index)
    risk_free = risk_free.loc[co_time]
    portfolio_return = portfolio_return.loc[co_time]
    excess_return = portfolio_return.apply(lambda x: x - risk_free)
    sharpe = excess_return.mean() / excess_return.std()
    annual_sharpe = np.sqrt(12) * sharpe
    annual_sharpe.name = 'annual_sharpe'
    return annual_sharpe

def get_annual_return(portfolio_return):
    annual_return = (portfolio_return.mean() + 1) ** ((portfolio_return.index[-1] - portfolio_return.index[0]).days / 365) - 1
    annual_return.name = 'annual_return'
    return annual_return

def get_annual_volatility(portfolio_return):
    annual_volatility = portfolio_return.std() * (12 ** .5)
    annual_volatility.name = 'annual_volatility'
    return annual_volatility

def get_β(portfolio_return):
    portfolio_return = portfolio_return.dropna()
    β = portfolio_return.apply(lambda y: sm.OLS(y, portfolio_return.hs300).fit().params[0])
    β.name = 'β'
    return β

def get_past_year_mdd(portfolio_return, mdd_month):
    '''past year mdd
    param: portfolio_return: pd.Series
    param: mdd_month: int
    return: pd.Series
    '''
    last_month = pd.to_datetime('today').normalize() - pd.offsets.MonthBegin(2)
    one_year_before_last_month = last_month - pd.offsets.MonthBegin(11)
    portfolio_return = portfolio_return.sort_index().loc[one_year_before_last_month: last_month]
    portfolio_return = portfolio_return.sort_index(ascending=False)
    mdd = (portfolio_return + 1).cumprod().rolling(mdd_month).apply(lambda x: (x[-1] - x.max())/x[-1]).replace([np.inf, -np.inf], np.nan).dropna().min()
    mdd.name = f'past_year_mdd'
    return mdd

def get_mdd(portfolio_return, mdd_month):
    portfolio_return = portfolio_return.sort_index(ascending=False)
    mdd = (portfolio_return + 1).cumprod().rolling(mdd_month).apply(lambda x: (x[-1] - x.max())/x[-1]).replace([np.inf, -np.inf], np.nan).dropna().min()
    mdd.name = f'mdd'
    return mdd
