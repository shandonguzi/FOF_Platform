
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_is = get_sql(level1_csmar, 'FS_Comins')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_is, Stock_last_day


def handle(data_is, Stock_last_day):

    data_is = data_is[data_is.ReportType == 'A'][['Stkcd', 'Date', 'Revenue', 'OperatingProfit']]
    data_is['PM'] = data_is['OperatingProfit'] / data_is['Revenue']
    
    pm = data_is[['Stkcd', 'Date', 'PM']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = pm.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    pm = pd.concat([pm, to_today], axis=0)
    pm = pd.merge(pm, Stock_last_day, on=['Stkcd'])
    pm = pm.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    pm = pm[(pm.Date <= pm.LastDay)]
    pm = pm.drop(['LastDay'], axis=1)
    pm['Date'] = pm['Date'] + pd.offsets.MonthBegin(0)

    return pm


def upload(pm):
    pm.to_sql('PM', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/PM')
def PM():
    data_is, Stock_last_day = read_sql()
    pm = handle(data_is, Stock_last_day)
    upload(pm)