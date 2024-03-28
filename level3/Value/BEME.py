
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    TRD_Mnth = get_sql(level1_csmar, 'TRD_Mnth')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, TRD_Mnth, Stock_last_day


def handle(data_bs, TRD_Mnth, Stock_last_day):

    data_bs = data_bs[data_bs.ReportType == 'A'][['Stkcd', 'Date', 'TotalShrhldrEquity', 'MinorityInterest']]
    data_bs['TotalShrhldrEquityExcMin'] = data_bs['TotalShrhldrEquity'] - data_bs['MinorityInterest']
    data_bs = data_bs.drop(['TotalShrhldrEquity', 'MinorityInterest'], axis=1)
    to_today = data_bs.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    data_bs = pd.concat([data_bs, to_today], axis=0)
    data_bs = data_bs.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    
    TRD_Mnth = TRD_Mnth[['Stkcd', 'Date', 'MKTValue']]
    TRD_Mnth.loc[:, 'Date'] = TRD_Mnth['Date'] + pd.offsets.MonthEnd(0)
    to_today = TRD_Mnth.sort_values(['Date']).groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_end)
    TRD_Mnth = pd.concat([TRD_Mnth, to_today], axis=0)
    df_merge = pd.merge(data_bs, TRD_Mnth, on=['Stkcd', 'Date'])
    df_merge['BEME'] = df_merge['TotalShrhldrEquityExcMin'] / df_merge['MKTValue']
    
    beme = df_merge[['Stkcd', 'Date', 'BEME']].replace([np.inf, -np.inf], np.nan).dropna()
    beme = pd.merge(beme, Stock_last_day, on=['Stkcd'])
    beme['Date'] = beme['Date'] - pd.offsets.MonthBegin(1)
    beme = beme[(beme.Date <= beme.LastDay)]
    beme = beme.drop(['LastDay'], axis=1)

    return beme


def upload(beme):
    beme.to_sql('BEME', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/BEME')
def BEME():
    data_bs, TRD_Mnth, Stock_last_day = read_sql()
    beme = handle(data_bs, TRD_Mnth, Stock_last_day)
    upload(beme)