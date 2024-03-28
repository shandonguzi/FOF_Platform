
from functools import reduce

import numpy as np
import pandas as pd
from parallel_pandas import ParallelPandas
from sklearn.linear_model import LinearRegression

from settings.database import level1_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

ParallelPandas.initialize(8, disable_pr_bar=True)

def read_sql():

    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')
    FF3 = get_sql(level1_csmar, 'FF3', index_cols='Date')
    risk_free = get_sql(level1_csmar, 'BND_Exchange', index_cols='Date')
    
    return TRD_Dalyr, FF3, risk_free


def ff_ε_generation(each_month, TRD_Dalyr, FF3, risk_free):

    stock_data = TRD_Dalyr[TRD_Dalyr.Stkcd == each_month['Stkcd']].sort_values('Date').drop('Stkcd', axis=1)
    regression_data = stock_data[stock_data.Date <= each_month['Date']][-30:].set_index('Date')

    temp_dates = regression_data.index
    temp_regression = FF3.loc[temp_dates]
    rf = risk_free.loc[temp_dates, 'riskfree_d']
    try:
        result = LinearRegression().fit(X = temp_regression[['MKT', 'SMB', 'HML']].values, y = (regression_data['RealPctChange'] - rf).values)
        ε = (result.predict(temp_regression[['MKT', 'SMB', 'HML']].values) - (regression_data['RealPctChange'] - rf).values).std()
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'IdioVol': ε})
    except:
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'IdioVol': np.nan})
    

def handle(TRD_Dalyr, FF3, risk_free):

    TRD_Dalyr = TRD_Dalyr[['Date', 'Stkcd', 'RealPctChange']].drop_duplicates(['Date', 'Stkcd'])
    TRD_Dalyr = TRD_Dalyr[(TRD_Dalyr.Date.isin(risk_free.dropna().index) & (TRD_Dalyr.Date.isin(FF3.dropna().index)))]
    month_trading = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('M').last().drop('Stkcd', axis=1).reset_index()
    
    idiovol = month_trading.p_apply(lambda x: ff_ε_generation(x, TRD_Dalyr, FF3, risk_free), axis=1)
    idiovol['Date'] = idiovol['Date'] - pd.offsets.MonthBegin(1)
    idiovol = idiovol.dropna()
    idiovol.name = 'idiovol'

    return idiovol


def upload(idiovol):
    idiovol.to_sql('IdioVol', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/IdioVol')
def IdioVol():
    TRD_Dalyr, FF3, risk_free = read_sql()
    idiovol = handle(TRD_Dalyr, FF3, risk_free)
    upload(idiovol)