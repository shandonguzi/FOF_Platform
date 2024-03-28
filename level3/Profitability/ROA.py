
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
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'NetProfit', 'TotalAssets']]
    finan_rept['ROA'] = finan_rept['NetProfit'] / finan_rept['TotalAssets']
    
    roa = finan_rept[['Stkcd', 'Date', 'ROA']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = roa.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    roa = pd.concat([roa, to_today], axis=0)
    roa = roa.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    roa = pd.merge(roa, Stock_last_day, on=['Stkcd'])
    roa = roa[(roa.Date <= roa.LastDay)]
    roa = roa.drop(['LastDay'], axis=1)
    roa['Date'] = roa['Date'] + pd.offsets.MonthBegin(0)

    return roa


def upload(roa):
    roa.to_sql('ROA', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ROA')
def ROA():
    data_is, data_bs, Stock_last_day = read_sql()
    roa = handle(data_is, data_bs, Stock_last_day)
    upload(roa)