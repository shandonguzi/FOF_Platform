
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_is = get_sql(level1_csmar, 'FS_Comins')
    TRD_Mnth = get_sql(level1_csmar, 'TRD_Mnth')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_is, TRD_Mnth, Stock_last_day


def handle(data_is, TRD_Mnth, Stock_last_day):

    data_is = data_is[(data_is.ReportType == 'A') & (data_is.Date.dt.month == 12)][['Stkcd', 'Date', 'Revenue']]
    to_today = data_is.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    data_is = pd.concat([data_is, to_today], axis=0)
    data_is = data_is.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    
    TRD_Mnth = TRD_Mnth[['Stkcd', 'Date', 'MKTValue']]
    TRD_Mnth.loc[:, 'Date'] = TRD_Mnth['Date'] + pd.offsets.MonthEnd(0)
    to_today = TRD_Mnth.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    TRD_Mnth = pd.concat([TRD_Mnth, to_today], axis=0)
    df_merge = pd.merge(data_is, TRD_Mnth, on=['Stkcd', 'Date'])
    df_merge['S2P'] = df_merge['Revenue'] / df_merge['MKTValue']

    s2p = df_merge[['Stkcd', 'Date', 'S2P']].replace([np.inf, -np.inf], np.nan).dropna()
    s2p = pd.merge(s2p, Stock_last_day, on=['Stkcd'])
    s2p['Date'] = s2p['Date'] - pd.offsets.MonthBegin(1)
    s2p = s2p[(s2p.Date <= s2p.LastDay)]
    s2p = s2p.drop(['LastDay'], axis=1)

    return s2p


def upload(s2p):
    s2p.to_sql('S2P', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/S2P')
def S2P():
    data_is, TRD_Mnth, Stock_last_day = read_sql()
    s2p = handle(data_is, TRD_Mnth, Stock_last_day)
    upload(s2p)