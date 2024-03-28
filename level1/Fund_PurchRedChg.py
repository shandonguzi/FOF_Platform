'''For get fund main info'''
import time

import numpy as np
import pandas as pd

import utils.mysql.p_to_sql
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'FUND_PurchRedChg')
    try:
        df_past = get_sql(level1_csmar, 'Fund_PurchRedChg', index_cols=['Date', 'Symbol'])
        return df, df_past
    except:
        return df, np.nan

def handle(df, df_past):
    df['ChangeDate'] = pd.to_datetime(df.ChangeDate)
    df = df.rename(columns={'ChangeDate': 'Date'})
    df = df[['Symbol', 'PurchaseStatus', 'RedeemStatus', 'Date']]
    df = df.drop_duplicates(['Symbol', 'Date'])
    PurchaseStatus = df.pivot(index='Date', columns='Symbol', values='PurchaseStatus').resample('d').last().fillna(method='ffill')
    RedeemStatus = df.pivot(index='Date', columns='Symbol', values='RedeemStatus').resample('d').last().fillna(method='ffill')
    PurchaseStatus.name = 'PurchaseStatus'
    RedeemStatus.name = 'RedeemStatus'
    from functools import reduce
    df = reduce(np.logical_or, [PurchaseStatus == 2, PurchaseStatus == 3, RedeemStatus == 2, RedeemStatus == 3]).replace(False, np.nan).stack()
    df.name = 'Bad'
    df = df.astype(bool).astype(int)

    if type(df_past) == float:
        pass
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
    return df

def upload(df, df_past):

    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 基金申购赎回状态变更表/Fund_PurchRedChg')
        df.reset_index().p_to_sql('Fund_PurchRedChg', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df) != 0:
            df.to_sql('Fund_PurchRedChg', con=level1_csmar, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/基金申购赎回状态变更表/Fund_PurchRedChg')
def Fund_PurchRedChg():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
