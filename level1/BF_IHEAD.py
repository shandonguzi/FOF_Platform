
import time

import numpy as np
import pandas as pd
from sqlalchemy.types import VARCHAR

import utils.mysql.p_to_sql
from utils.frequent_dates import last_month_begin_s
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from utils.mysql.del_sql import del_sql

def read_sql():
    '''basic io'''
    try:
        df = get_sql(level0_csmar, f'select * from BF_IHEAD where Trddt > "{last_month_begin_s}"')
        df_past = get_sql(level1_csmar, f'select * from BF_IHEAD where Date > "{last_month_begin_s}"')
        return df, df_past
    except:
        df = get_sql(level0_csmar, 'BF_IHEAD')
        return df, np.nan
    
def handle(df, df_past):
    df = df.rename(columns={'Nnindcd': 'CSRC_Clsf', 'Trddt': 'Date', 'Dretwdeq': 'RealPctChange_equal', 'Dretwdos': 'RealPctChange_currency', 'Dretwdtl': 'RealPctChange_total', 'CSAD1': 'CSAD_equal', 'CSAD3': 'CSAD_currency', 'CSAD5': 'CSAD_total'})
    df['Date'] = pd.to_datetime(df.Date)
    df = df.dropna()
    df = df[df['ST'] != 0]
    df = df.drop('ST', axis=1)
    if type(df_past) == float:
        return df
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
        return df
    
def upload(df, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 股票行业羊群效应/BF_IHEAD')
        df.set_index(['Date', 'CSRC_Clsf'],inplace=True)
        # df.p_to_sql('BF_IHEAD', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'CSRC_Clsf'])
        df.to_sql('BF_IHEAD', con=level1_csmar, dtype={'CSRC_Clsf': VARCHAR(length=4)}, if_exists='replace', index=True)
    else:
        if len(df) != 0:
            # df.p_to_sql('BF_IHEAD', level1_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df.to_sql('BF_IHEAD', con=level1_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/股票行业羊群效应/BF_IHEAD')
def BF_IHEAD():
    # del_sql(level1_csmar,'BF_IHEAD')
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
