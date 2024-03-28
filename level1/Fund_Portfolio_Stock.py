'''For get fund main info'''

import time

import numpy as np
import pandas as pd

import utils.mysql.p_to_sql
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from utils.frequent_dates import last_6_month_begin

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, f'select * from Fund_Portfolio_Stock where EndDate > "{last_6_month_begin}"')
    try:
        df_past = get_sql(
            level1_csmar,
            f'select * from Fund_Portfolio_Stock where Date > "{last_6_month_begin}"',
        )
        return df, df_past
    except Exception:
        return df, np.nan

def handle(df, df_past):
    df['EndDate'] = pd.to_datetime(df.EndDate)
    df = df.rename(columns={'Symbol': 'Stkcd', 'EndDate': 'Date'})
    symbol_code_mapping = get_sql(level0_csmar, 'FUND_FundCodeInfo').drop_duplicates(['MasterFundCode', 'Symbol'])[['MasterFundCode', 'Symbol']]
    df = pd.merge(df, symbol_code_mapping, on='MasterFundCode', how='inner')
    df = df.drop_duplicates(['Date', 'Symbol', 'Stkcd'])
    df = df[['Date', 'Symbol', 'Stkcd', 'StockName', 'Proportion']]
    df = df.fillna(0)
    df['Stkcd'] = pd.to_numeric(df.Stkcd, errors='coerce')
    df = df.replace([np.inf, np.nan]).replace([- np.inf, np.nan]).dropna()
    df['Stkcd'] = df.Stkcd.astype(int)
    if type(df_past) != float:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
    return df

def upload(df, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 股票投资明细表/Fund_Portfolio_Stock')
        df.p_to_sql('Fund_Portfolio_Stock', level1_csmar, partitions=1000, n_workers=1, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df) != 0:
            # df.p_to_sql('Fund_Portfolio_Stock', level1_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df.to_sql('Fund_Portfolio_Stock', level1_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/股票投资明细表/Fund_Portfolio_Stock')
def Fund_Portfolio_Stock():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
