'''For get fund main info'''

import time

import numpy as np
import pandas as pd
from sqlalchemy.types import VARCHAR

import utils.mysql.p_to_sql
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic input'''
    df = get_sql(level0_csmar, 'Fund_Allocation')
    try:
        df_past_q = get_sql(level1_csmar, 'Fund_Allocation_q')
        df_past_m = get_sql(level1_csmar, 'Fund_Allocation_m')
        df_past = [df_past_q, df_past_m]
        return df, df_past
    except:
        return df, [np.nan, np.nan]

def handle(df, df_past):
    '''handle df'''
    df = df.rename(columns={'EndDate': 'Date'})
    df['Date'] = pd.to_datetime(df.Date)
    symbol_code_mapping = \
        get_sql(level0_csmar, 'FUND_FundCodeInfo').drop_duplicates(['MasterFundCode', 'Symbol'])[['MasterFundCode', 'Symbol']]
    df = \
        pd.merge(df.drop_duplicates(['MasterFundCode', 'Date', 'ReportTypeID']), symbol_code_mapping, on='MasterFundCode', how='inner')[['Date', 'Symbol', 'ReportTypeID', 'Equity', 'FixedIncome', 'TotalAsset']]
    df = df[df.TotalAsset != 0]
    df['ReportTypeID'] = df.ReportTypeID.replace(1, '第一季度')
    df['ReportTypeID'] = df.ReportTypeID.replace(2, '第二季度')
    df['ReportTypeID'] = df.ReportTypeID.replace(3, '第三季度')
    df['ReportTypeID'] = df.ReportTypeID.replace(4, '第四季度')
    df['ReportTypeID'] = df.ReportTypeID.replace(5, '半年度')
    df['ReportTypeID'] = df.ReportTypeID.replace(6, '年度')
    df['ReportTypeID'] = df.ReportTypeID.replace(7, '其他周期')
    df['Equity'] = df.Equity.fillna(0)
    df['FixedIncome'] = df.FixedIncome.fillna(0)
    df['EquityProportion'] = df.Equity / df.TotalAsset
    df['FixedIncomeProportion'] = df.FixedIncome / df.TotalAsset
    df['EquityAndFixedIncomeProportion'] = df.EquityProportion + df.FixedIncomeProportion
    df = df.drop_duplicates(['Symbol', 'Date'])
    last_quarter_end = df.Date.nlargest(1).values[0]
    this_quarter = df[df.Date == last_quarter_end].copy()
    this_quarter['Date'] = this_quarter.Date + pd.offsets.QuarterEnd(1)
    df = pd.concat([df, this_quarter])
    df_m = df.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').ffill()).drop('Symbol', axis=1)
    df_q = df.set_index(['Date', 'Symbol'])

    df_q_past = df_past[0]
    df_m_past = df_past[1]

    if type(df_q_past) != float:
        df_q = pd.concat([df_q.reset_index(), df_q_past]).drop_duplicates(keep=False)

    if type(df_m_past) != float:
        df_m = pd.concat([df_m.reset_index(), df_m_past]).drop_duplicates(keep=False)

    return df_q, df_m

def upload(df_q, df_m, past):
    if type(past[0]) == float:
        print(f'[+] {time.strftime("%c")} init 资产配置/Fund_Allocation_q')
        df_q.p_to_sql('Fund_Allocation_q', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df_q) != 0:
            df_q.set_index(['Date', 'Symbol']).to_sql('Fund_Allocation_q', con=level1_csmar, dtype={'ReportTypeID': VARCHAR(length=4)}, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

    if type(past[1]) == float:
        print(f'[+] {time.strftime("%c")} init 资产配置/Fund_Allocation_m')
        df_m.p_to_sql('Fund_Allocation_m', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df_m) != 0:
            df_m.set_index(['Date', 'Symbol']).to_sql('Fund_Allocation_m', con=level1_csmar, dtype={'ReportTypeID': VARCHAR(length=4)}, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')


@timeit('level1/资产配置/Fund_Allocation')
def Fund_Allocation():
    df, df_past = read_sql()
    df_q, df_m = handle(df, df_past)
    upload(df_q, df_m, df_past)
