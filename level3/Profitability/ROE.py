
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

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer', suffixes=('_is', '_bs')), [data_is, data_bs])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'NetProfit', 'TotalShrhldrEquity', 'MinorityInterest_bs']]
    finan_rept['ROE'] = finan_rept['NetProfit'] / (finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest_bs'])
    
    roe = finan_rept[['Stkcd', 'Date', 'ROE']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = roe.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    roe = pd.concat([roe, to_today], axis=0)
    roe = roe.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    roe = pd.merge(roe, Stock_last_day, on=['Stkcd'])
    roe = roe[(roe.Date <= roe.LastDay)]
    roe = roe.drop(['LastDay'], axis=1)
    roe['Date'] = roe['Date'] + pd.offsets.MonthBegin(0)

    return roe


def upload(roe):
    roe.to_sql('ROE', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ROE')
def ROE():
    data_is, data_bs, Stock_last_day = read_sql()
    roe = handle(data_is, data_bs, Stock_last_day)
    upload(roe)