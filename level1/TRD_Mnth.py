
import time

import numpy as np
import pandas as pd
import utils.mysql.p_to_sql
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'TRD_Mnth')
    try:
        df_past = get_sql(level1_csmar, 'TRD_Mnth')
        return df, df_past
    except:
        return df, np.nan

def handle(df, df_past):
    df = df.rename(columns={'Trdmnt': 'Date', 'Msmvttl': 'MKTValue', 'Mretwd': 'RealPctChange', 'Mclsprc': 'Close', 'Mnshrtrd': 'TradingVolume'})
    df['Date'] = pd.to_datetime(df.Date)
    df = df.dropna()
    if type(df_past) == float:
        return df
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
        return df

def upload(df, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 月个股回报率/TRD_Mnth')
        df.p_to_sql('TRD_Mnth', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(df) != 0:
            df.set_index(['Date', 'Stkcd']).to_sql('TRD_Mnth', level1_csmar, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/月个股回报率/TRD_Mnth')
def TRD_Mnth():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
