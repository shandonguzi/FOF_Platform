
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

    Fund_TNA = get_sql(level3_factors, 'Fund_TNA')
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m')
    
    return Fund_TNA, Fund_Return_m


def handle(Fund_TNA, Fund_Return_m):

    Fund_Return_m['Date'] = Fund_Return_m['Date'] - pd.offsets.MonthBegin(1)
    Fund_flow = pd.merge(Fund_TNA, Fund_Return_m, on=['Date', 'Symbol'], how='inner')
    Fund_flow = Fund_flow.set_index('Date').groupby('Symbol').resample('M').last().drop('Symbol', axis=1)
    Fund_flow['lagTNA'] = Fund_flow.groupby(level=0)['TNA'].shift(1)
    Fund_flow['flow'] = (Fund_flow['TNA'] - Fund_flow['lagTNA'] * Fund_flow['Fund_Return_m']) / Fund_flow['lagTNA']
    Fund_flow = Fund_flow.reset_index()[['Symbol', 'Date', 'flow']].dropna()
    Fund_flow = Fund_flow.replace([np.inf, -np.inf], np.nan).dropna()
    Fund_flow['Date'] = Fund_flow['Date'] - pd.offsets.MonthBegin(1)

    return Fund_flow


def upload(Fund_flow):
    Fund_flow.to_sql('Fund_FLOW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Fund_FLOW')
def Fund_FLOW():
    '''
    貌似没什么用这个因子
    to check
    '''
    Fund_TNA, Fund_Return_m = read_sql()
    Fund_flow = handle(Fund_TNA, Fund_Return_m)
    upload(Fund_flow)
Fund_FLOW()