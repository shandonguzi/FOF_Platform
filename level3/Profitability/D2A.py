
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    data_cfi = get_sql(level1_csmar, 'FS_Comscfi')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, data_cfi, Stock_last_day


def handle(data_bs, data_cfi, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_bs, data_cfi])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'TotalAssets', 'DepreciationFaOgaBba', 'AmortizationIntang']]
    finan_rept['D2A'] = (finan_rept['DepreciationFaOgaBba'] + finan_rept['AmortizationIntang']) / finan_rept['TotalAssets']
    
    d2a = finan_rept[['Stkcd', 'Date', 'D2A']].replace([np.inf, -np.inf], np.nan).dropna()    
    to_today = d2a.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    d2a = pd.concat([d2a, to_today], axis=0)
    d2a = pd.merge(d2a, Stock_last_day, on=['Stkcd'])
    d2a = d2a.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    d2a = d2a[(d2a.Date <= d2a.LastDay)]
    d2a = d2a.drop(['LastDay'], axis=1)
    d2a['Date'] = d2a['Date'] + pd.offsets.MonthBegin(0)

    return d2a


def upload(d2a):
    d2a.to_sql('D2A', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/D2A')
def D2A():
    data_bs, data_cfi, Stock_last_day = read_sql()
    d2a = handle(data_bs, data_cfi, Stock_last_day)
    upload(d2a)