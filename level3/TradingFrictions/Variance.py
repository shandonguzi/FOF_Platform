
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
    
    return TRD_Dalyr


def variance_generation(each_month, TRD_Dalyr):

    stock_data = TRD_Dalyr[TRD_Dalyr.Stkcd == each_month['Stkcd']].sort_values('Date').drop('Stkcd', axis=1)
    compute_data = stock_data[stock_data.Date <= each_month['Date']][-60:].set_index('Date')

    try:
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'Variance': compute_data.var()})
    except:
        return pd.Series({'Stkcd': each_month['Stkcd'], 'Date': each_month['Date'], 'Variance': np.nan})
    

def handle(TRD_Dalyr):

    TRD_Dalyr = TRD_Dalyr[['Date', 'Stkcd', 'RealPctChange']].drop_duplicates(['Date', 'Stkcd'])
    month_trading = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('M').last().drop('Stkcd', axis=1).reset_index()
    
    variance = month_trading.p_apply(lambda x: variance_generation(x, TRD_Dalyr), axis=1)
    variance['Date'] = variance['Date'] - pd.offsets.MonthBegin(1)
    variance = variance.dropna()
    variance.name = 'variance'

    return variance


def upload(variance):
    variance.to_sql('Variance', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Variance')
def Variance():
    TRD_Dalyr= read_sql()
    variance = handle(TRD_Dalyr)
    upload(variance)