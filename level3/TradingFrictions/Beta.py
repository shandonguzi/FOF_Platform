
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from numpy.lib.stride_tricks import as_strided as stride


def read_sql():

    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')
    risk_free = get_sql(level1_csmar, 'BND_Exchange')
    
    return TRD_Dalyr, risk_free


# df.pipe(roll, w=60).apply(lambda x: x['MarketLogExcess_3m'].corr(x['StockLogExcess_3m']))，
# 此函数也可实现rolling多列功能，todo：add min_periods parameter
def roll(df, w, **kwargs):
    v = df.values
    d0, d1 = v.shape
    s0, s1 = v.strides

    a = stride(v, (d0 - (w - 1), w, d1), (s0, s0, s1))

    rolled_df = pd.concat({
        row: pd.DataFrame(values, columns=df.columns)
        for row, values in zip(df.index, a)
    })

    return rolled_df.groupby(level=0, **kwargs)


def handle(TRD_Dalyr, risk_free):
    Market = TRD_Dalyr.groupby(['Date']).apply(lambda x: x['RealPctChange'].mean()).reset_index().rename(columns={0: 'MarketReturn'})
    Market = pd.merge(Market, risk_free, on=['Date'], how='inner')
    Market['MarketLogExcess'] = np.log(1 + Market['MarketReturn'] - Market['riskfree_d'])
    Market = Market[['Date', 'MarketLogExcess']].set_index(['Date']).sort_index().resample('M').ffill()
    Market['volatility_m'] = Market['MarketLogExcess'].rolling(12).std()
    Market = Market.reset_index()
    # 日度时间太长，改月度因子
    # Market['volatility_m'] = Market['MarketLogExcess'].rolling(252, min_periods=120).std()

    TRD_Dalyr = TRD_Dalyr[['Stkcd', 'Date', 'RealPctChange']].drop_duplicates(['Stkcd', 'Date'])
    TRD_Dalyr = pd.merge(TRD_Dalyr, risk_free, on=['Date'], how='inner')
    TRD_Dalyr['StockLogExcess'] = np.log(1 + TRD_Dalyr['RealPctChange'] - TRD_Dalyr['riskfree_d'])
    TRD_Dalyr = TRD_Dalyr[['Stkcd', 'Date', 'StockLogExcess']].sort_values(['Stkcd', 'Date'])
    TRD_Dalyr = TRD_Dalyr.groupby('Stkcd').apply(lambda x: x.set_index('Date').resample('M').ffill()).drop(['Stkcd'], axis=1).reset_index()
    TRD_Dalyr['volatility_s'] = TRD_Dalyr.groupby('Stkcd').apply(lambda x: x.set_index('Date').rolling(12).std()).drop('Stkcd', axis=1).reset_index(drop=True)
    # TRD_Dalyr['volatility_s'] = TRD_Dalyr.groupby('Stkcd').apply(lambda x: x.set_index('Date').rolling(252, min_periods=120).std()).drop('Stkcd', axis=1).reset_index(drop=True)
    
    beta = pd.merge(TRD_Dalyr, Market, on=['Date'], how='inner').sort_values(['Stkcd', 'Date']).set_index(['Stkcd', 'Date'])
    beta['volatility_ratio'] = beta['volatility_s'] / beta['volatility_m']
    beta['MarketLogExcess_3m'] = beta[['MarketLogExcess']].groupby(level=0).rolling(3).apply(lambda x: x.sum()).droplevel(0)
    beta['StockLogExcess_3m'] = beta[['StockLogExcess']].groupby(level=0).rolling(3).apply(lambda x: x.sum()).droplevel(0)

    # 不必rolling两列，https://stackoverflow.com/questions/40453602/deprecated-pd-rolling-corr-into-pandas-19/40453698#40453698
    beta['correlation'] = beta[['MarketLogExcess_3m', 'StockLogExcess_3m']].groupby(level=0).apply(lambda x: x['MarketLogExcess_3m'].rolling(60, min_periods=36).corr(x['StockLogExcess_3m'])).droplevel(0)
    beta['CAPM_beta'] = beta['correlation'] * beta['volatility_ratio']
    beta = beta[['CAPM_beta']].reset_index().dropna()
    beta['Date'] = beta['Date'] - pd.offsets.MonthBegin(1)
    
    return beta


def upload(beta):
    beta.to_sql('CAPM_beta', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CAPM_beta')
def CAPM_beta():
    TRD_Dalyr, risk_free = read_sql()
    beta = handle(TRD_Dalyr, risk_free)
    upload(beta)