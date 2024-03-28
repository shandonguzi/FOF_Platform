
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

    data_bs = data_bs[(data_bs.ReportType == 'A') & (data_bs.Date.dt.month == 12)][['Stkcd', 'Date', 'Inventory', 'PPE', 'TotalAssets']]
    data_bs = data_bs.set_index(['Date']).groupby(['Stkcd']).resample('Y').ffill().dropna().drop(['Stkcd'], axis=1)
    data_bs['LastYearInventory'] = data_bs.groupby(['Stkcd'])['Inventory'].shift(1)
    data_bs['LastYearPPE'] = data_bs.groupby(['Stkcd'])['PPE'].shift(1)
    data_bs['LastYearTotalAssets'] = data_bs.groupby(['Stkcd'])['TotalAssets'].shift(1)
    data_bs['DPI2A'] = (data_bs['Inventory'] - data_bs['LastYearInventory'] + data_bs['PPE'] - data_bs['LastYearPPE']) / data_bs['LastYearTotalAssets']
    
    dpi2a = data_bs.reset_index()[['Stkcd', 'Date', 'DPI2A']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = dpi2a.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    dpi2a = pd.concat([dpi2a, to_today], axis=0)
    dpi2a = pd.merge(dpi2a, Stock_last_day, on=['Stkcd'])
    dpi2a = dpi2a.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    dpi2a = dpi2a[(dpi2a.Date <= dpi2a.LastDay)]
    dpi2a = dpi2a.drop(['LastDay'], axis=1)
    dpi2a['Date'] = dpi2a['Date'] + pd.offsets.MonthBegin(0)

    return dpi2a


def upload(dpi2a):
    dpi2a.to_sql('DPI2A', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/DPI2A')
def DPI2A():
    data_bs, Stock_last_day = read_sql()
    dpi2a = handle(data_bs, Stock_last_day)
    upload(dpi2a)