
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

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer',suffixes=('_is', '_bs')), [data_is, data_bs])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'Revenue', 'COGS', 'TotalShrhldrEquity', 'MinorityInterest_bs']]
    finan_rept['PROF'] = (finan_rept['Revenue'] - finan_rept['COGS']) / (finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest_bs'])
    
    prof = finan_rept[['Stkcd', 'Date', 'PROF']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = prof.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    prof = pd.concat([prof, to_today], axis=0)
    prof = prof.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    prof = pd.merge(prof, Stock_last_day, on=['Stkcd'])
    prof = prof[(prof.Date <= prof.LastDay)]
    prof = prof.drop(['LastDay'], axis=1)
    prof['Date'] = prof['Date'] + pd.offsets.MonthBegin(0)

    return prof


def upload(prof):
    prof.to_sql('PROF', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/PROF')
def PROF():
    data_is, data_bs, Stock_last_day = read_sql()
    prof = handle(data_is, data_bs, Stock_last_day)
    upload(prof)