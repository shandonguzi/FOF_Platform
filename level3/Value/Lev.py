
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, Stock_last_day


def handle(data_bs, Stock_last_day):

    data_bs = data_bs[data_bs.ReportType == 'A'][['Stkcd', 'Date', 'ShortTermBorrow', 'LongTermBorrow', 'TotalShrhldrEquity']]
    data_bs['Lev'] = (data_bs['ShortTermBorrow'] + data_bs['LongTermBorrow']) / (data_bs['ShortTermBorrow'] + data_bs['LongTermBorrow'] + data_bs['TotalShrhldrEquity'])
    
    lev = data_bs[['Stkcd', 'Date', 'Lev']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = lev.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    lev = pd.concat([lev, to_today], axis=0)
    lev = pd.merge(lev, Stock_last_day, on=['Stkcd'])
    lev = lev.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    lev = lev[(lev.Date <= lev.LastDay)]
    lev = lev.drop(['LastDay'], axis=1)
    lev['Date'] = lev['Date'] + pd.offsets.MonthBegin(0)

    return lev


def upload(lev):
    lev.to_sql('Lev', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Lev')
def Lev():
    data_bs, Stock_last_day = read_sql()
    lev = handle(data_bs, Stock_last_day)
    upload(lev)