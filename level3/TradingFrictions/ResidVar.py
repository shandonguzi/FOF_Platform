
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


def ff_var_generation(each_month, TRD_Dalyr, FF3, risk_free):

    stock_data = TRD_Dalyr[TRD_Dalyr.Stkcd == each_month['Stkcd']].sort_values('Date').drop('Stkcd', axis=1)
    regression_data = stock_data[stock_data.Date <= each_month['Date']][-60:].set_index('Date')

    temp_dates = regression_data.index
    temp_regression = FF3.loc[temp_dates]
    rf = risk_free.loc[temp_dates, 'riskfree_d']
    try:
        result = LinearRegression().fit(X = temp_regression[['MKT', 'SMB', 'HML']].values, y = (regression_data['RealPctChange'] - rf).values)
        var = (result.predict(temp_regression[['MKT', 'SMB', 'HML']].values) - (regression_data['RealPctChange'] - rf).values).var()
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'ResidVar': var})
    except:
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'ResidVar': np.nan})
    

def handle(TRD_Dalyr, FF3, risk_free):

    TRD_Dalyr = TRD_Dalyr[['Date', 'Stkcd', 'RealPctChange']].drop_duplicates(['Date', 'Stkcd'])
    TRD_Dalyr = TRD_Dalyr[(TRD_Dalyr.Date.isin(risk_free.dropna().index) & (TRD_Dalyr.Date.isin(FF3.dropna().index)))]
    month_trading = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('M').last().drop('Stkcd', axis=1).reset_index()
    
    residvar = month_trading.p_apply(lambda x: ff_var_generation(x, TRD_Dalyr, FF3, risk_free), axis=1)
    residvar['Date'] = residvar['Date'] - pd.offsets.MonthBegin(1)
    residvar = residvar.dropna()
    residvar.name = 'residvar'

    return residvar


def upload(residvar):
    residvar.to_sql('ResidVar', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ResidVar')
def ResidVar():
    TRD_Dalyr, FF3, risk_free = read_sql()
    residvar = handle(TRD_Dalyr, FF3, risk_free)
    upload(residvar)