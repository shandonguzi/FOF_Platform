'''For get fund main info'''

import time

import numpy as np
import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'Fund_Ptf_BondSpe')
    try:
        df_past = get_sql(level1_csmar, 'Fund_Ptf_BondSpe')
        return df, df_past
    except:
        return df, np.nan

def handle(df, df_past):
    df['EndDate'] = pd.to_datetime(df.EndDate)
    df = df.rename(columns={'EndDate': 'Date', 'FairValueToNAV': 'Proportion'})
    symbol_code_mapping = \
        get_sql(level0_csmar, 'FUND_FundCodeInfo').drop_duplicates(['MasterFundCode', 'Symbol'])[['MasterFundCode', 'Symbol']]
    df = \
        pd.merge(df, symbol_code_mapping, on='MasterFundCode', how='inner')
    df = df.drop_duplicates(['Date', 'Symbol', 'SpeciesName'])
    df = df[['Date', 'Symbol', 'SpeciesName', 'Proportion']].set_index(['Date', 'Symbol'])
    if type(df_past) == float:
        df = df.dropna()
        return df
    else:
        df = pd.concat([df.reset_index(), df_past]).drop_duplicates(keep=False)
        df = df.dropna()
        return df

def upload(df, df_past):
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 按品种分类的债券投资组合/Fund_Ptf_BondSpe')
        df.p_to_sql('Fund_Ptf_BondSpe', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df) != 0:
            df.p_to_sql('Fund_Ptf_BondSpe', level1_csmar, partitions=1, n_workers=1, threads_per_worker=1)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/按品种分类的债券投资组合/Fund_Ptf_BondSpe')
def Fund_Ptf_BondSpe():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
