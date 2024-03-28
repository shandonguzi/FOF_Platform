
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
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'Revenue', 'COGS', 'InterestExp', 'SellExp', 'AdminExp', 'TotalShrhldrEquity', 'MinorityInterest_bs']]
    finan_rept['OP'] = (finan_rept['Revenue'] - finan_rept['COGS'] - finan_rept['InterestExp'] - finan_rept['SellExp'] - finan_rept['AdminExp']) / (finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest_bs'])
    
    op = finan_rept[['Stkcd', 'Date', 'OP']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = op.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    op = pd.concat([op, to_today], axis=0)
    op = op.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    op = pd.merge(op, Stock_last_day, on=['Stkcd'])
    op = op[(op.Date <= op.LastDay)]
    op = op.drop(['LastDay'], axis=1)
    op['Date'] = op['Date'] + pd.offsets.MonthBegin(0)

    return op


def upload(op):
    op.to_sql('OP', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/OP')
def OP():
    data_is, data_bs, Stock_last_day = read_sql()
    op = handle(data_is, data_bs, Stock_last_day)
    upload(op)