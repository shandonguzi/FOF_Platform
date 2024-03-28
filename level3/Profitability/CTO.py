
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_is = get_sql(level1_csmar, 'FS_Comins')
    data_bs = get_sql(level1_csmar, 'FS_Combas')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_is, data_bs, Stock_last_day


def handle(data_is, data_bs, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_is, data_bs])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'Revenue', 'TotalAssets']]
    finan_rept['CTO'] = finan_rept['Revenue'] / finan_rept['TotalAssets']
    
    cto = finan_rept[['Stkcd', 'Date', 'CTO']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = cto.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    cto = pd.concat([cto, to_today], axis=0)
    cto = cto.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    cto = pd.merge(cto, Stock_last_day, on=['Stkcd'])
    cto = cto[(cto.Date <= cto.LastDay)]
    cto = cto.drop(['LastDay'], axis=1)
    cto['Date'] = cto['Date'] + pd.offsets.MonthBegin(0)

    return cto


def upload(cto):
    cto.to_sql('CTO', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CTO')
def CTO():
    data_is, data_bs, Stock_last_day = read_sql()
    cto = handle(data_is, data_bs, Stock_last_day)
    upload(cto)