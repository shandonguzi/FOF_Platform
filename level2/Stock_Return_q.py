'''For get fund main info'''

import time

import numpy as np
import pandas as pd
from settings.database import level1_csmar, level2_csmar

import utils.mysql.p_to_sql
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')

    try:
        df_past = get_sql(level2_csmar, 'Stock_Return_q')
        return df_past, TRD_Dalyr
    except Exception:
        return np.nan, TRD_Dalyr

def handle(df_past, TRD_Dalyr):
    TRD_Dalyr = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('Q').last().droplevel(0)
    TRD_Dalyr['close_shift'] = TRD_Dalyr['Close'].shift(-1)
    TRD_Dalyr['return_q'] = (TRD_Dalyr['close_shift'] - TRD_Dalyr['Close'])/TRD_Dalyr['Close']
    TRD_Dalyr.reset_index(inplace=True)
    TRD_Dalyr = TRD_Dalyr[['Date', 'Stkcd', 'return_q']]

    TRD_Dalyr.dropna(inplace=True)

    if type(df_past) == float:
        return TRD_Dalyr
    else:
        TRD_Dalyr = pd.concat([TRD_Dalyr, df_past]).drop_duplicates(keep=False)
        return TRD_Dalyr

def upload(df_return, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Stock_Return_q')
        df_return.p_to_sql('Stock_Return_q', level2_csmar, partitions=1000, n_workers=12, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(df_return) != 0:
            df_return.p_to_sql('Stock_Return_q', level2_csmar, partitions=1, n_workers=1, threads_per_worker=1)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level2/Stock_Return_q')
def Stock_Return_q():
    df_past, TRD_Dalyr = read_sql()
    df_return = handle(df_past, TRD_Dalyr)
    upload(df_return, df_past)