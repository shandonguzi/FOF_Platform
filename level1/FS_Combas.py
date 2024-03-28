
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
        df = get_sql(level0_csmar, f'select * from FS_Combas where Accper > "{last_month_begin_s}"')
        df_past = get_sql(level1_csmar, f'select * from FS_Combas where Date > "{last_month_begin_s}"')
        return df, df_past
    except:
        df = get_sql(level0_csmar, 'FS_Combas')
        return df, np.nan

def handle(df, df_past):
    df = df.rename(columns={'Accper': 'Date', 'Typrep': 'ReportType', 'A001101000': 'MonetaryCapital', 
                            'A001107000': 'TradingFinAssets', 'A001100000': 'TotalCurAssets', 'A001000000': 'TotalAssets', 
                            'A002101000': 'ShortTermBorrow', 'A002105000': 'TradingFinLiab', 'A002107000': 'NotesPayable',
                            'A002113000': 'TaxesPayable', 'A002125000': 'NonCurLiabIn1Y', 'A002100000': 'TotalCurLiab',
                            'A002201000': 'LongTermBorrow', 'A003200000': 'MinorityInterest', 'A003000000': 'TotalShrhldrEquity',
                            'A001123000': 'Inventory', 'A001212000': 'PPE', 'A001109000': 'ShortTermInvestment', 'A001222000': 'DTA',
                            'A002208000': 'DTL'})
    df['Date'] = pd.to_datetime(df.Date)
    df = df.fillna(0)
    df = df.drop_duplicates()
    if type(df_past) == float:
        return df
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
        return df

def upload(df, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init 公司资产负债表/FS_Combas')
        df.p_to_sql('FS_Combas', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(df) != 0:
            df.p_to_sql('FS_Combas', level1_csmar, partitions=1, n_workers=1, threads_per_worker=1)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/公司资产负债表/FS_Combas')
def FS_Combas():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df, df_past)
