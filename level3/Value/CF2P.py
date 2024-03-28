
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_cfi = get_sql(level1_csmar, 'FS_Comscfi')
    TRD_Mnth = get_sql(level1_csmar, 'TRD_Mnth')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_cfi, TRD_Mnth, Stock_last_day


def handle(data_cfi, TRD_Mnth, Stock_last_day):

    data_cfi = data_cfi[(data_cfi.ReportType == 'A') & (data_cfi.Date.dt.month == 12)][['Stkcd', 'Date', 'NetIncome', 'DepreciationFaOgaBba', 'AmortizationIntang', 'DTA', 'DTL']]
    to_today = data_cfi.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    data_cfi = pd.concat([data_cfi, to_today], axis=0)
    data_cfi = data_cfi.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    
    TRD_Mnth = TRD_Mnth[['Stkcd', 'Date', 'MKTValue']]
    TRD_Mnth.loc[:, 'Date'] = TRD_Mnth['Date'] + pd.offsets.MonthEnd(0)
    to_today = TRD_Mnth.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    TRD_Mnth = pd.concat([TRD_Mnth, to_today], axis=0)
    df_merge = pd.merge(data_cfi, TRD_Mnth, on=['Stkcd', 'Date'])
    df_merge['CF2P'] = (df_merge['NetIncome'] + df_merge['DepreciationFaOgaBba'] + df_merge['AmortizationIntang'] + df_merge['DTA'] + df_merge['DTL']) / df_merge['MKTValue']
    
    cf2p = df_merge[['Stkcd', 'Date', 'CF2P']].replace([np.inf, -np.inf], np.nan).dropna()
    cf2p = pd.merge(cf2p, Stock_last_day, on=['Stkcd'])
    cf2p['Date'] = cf2p['Date'] - pd.offsets.MonthBegin(1)
    cf2p = cf2p[(cf2p.Date <= cf2p.LastDay)]
    cf2p = cf2p.drop(['LastDay'], axis=1)

    return cf2p


def upload(cf2p):
    cf2p.to_sql('CF2P', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CF2P')
def CF2P():
    data_cfi, TRD_Mnth, Stock_last_day = read_sql()
    cf2p = handle(data_cfi, TRD_Mnth, Stock_last_day)
    upload(cf2p)