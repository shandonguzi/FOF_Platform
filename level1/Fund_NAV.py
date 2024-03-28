
'''For get fund main info'''

import time

import numpy as np
import pandas as pd

import utils.mysql.p_to_sql
from utils.frequent_dates import last_month_begin_s
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    try:
        df = get_sql(level0_csmar, f'select * from Fund_NAV where TradingDate > "{last_month_begin_s}"')
        df_past = get_sql(level1_csmar, f'select * from Fund_NAV where Date > "{last_month_begin_s}"')
        return df, df_past
    except:
        df = get_sql(level0_csmar, 'Fund_NAV')
        return df, np.nan

def handle(df, df_past):
    df = df.rename(columns={'TradingDate': 'Date'})
    df['Date'] = pd.to_datetime(df.Date)
    df = df.fillna(1)
    if type(df_past) == float:
        return df
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
        return df

def upload(df, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 基金日净值/Fund_NAV')
        df.p_to_sql('Fund_NAV', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df) != 0:
            # df.p_to_sql('Fund_NAV', level1_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df.to_sql('Fund_NAV', level1_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/基金日净值/Fund_NAV')
def Fund_NAV():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
