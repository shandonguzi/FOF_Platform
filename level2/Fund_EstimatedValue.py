
'''For get fund main info'''

import time

import numpy as np
import pandas as pd

import utils.mysql.p_to_sql
from settings.database import level1_csmar, level2_csmar
from utils.frequent_dates import last_month_begin_s
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_nav_adj = get_sql(level2_csmar, 'Fund_NAV_adj')
    df_share = get_sql(level1_csmar, 'Fund_ShareChange')
    try:
        df_past = get_sql(level2_csmar, 'Fund_EstimatedValue')
        return df_nav_adj, df_share, df_past
    except:
        return df_nav_adj, df_share, np.nan

def fill_mkt_value(x):
    if len(x) != 1:
        x = x.sort_values('Date')
        x['EndDateShares'] = x.EndDateShares.fillna(method='ffill')
        return x
    else:
        return x.fillna(0)

def handle(df_nav_adj, df_share, df_past):
    df_value = pd.merge(df_nav_adj, df_share, on=['Date', 'Symbol'], how='left').groupby('Symbol').apply(fill_mkt_value).droplevel(0)
    df_value = df_value.dropna()
    df_value['EstimatedValue'] = df_value.NAV_adj * df_value.EndDateShares
    df_value = df_value[['Date', 'Symbol', 'EstimatedValue']]
    if type(df_past) == float:
        return df_value
    else:
        df_value = pd.concat([df_value, df_past]).drop_duplicates(keep=False)
        return df_value

def upload(df_value, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Fund_EstimatedValue')
        df_value.p_to_sql('Fund_EstimatedValue', level2_csmar, partitions=1000, n_workers=12, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df_value) != 0:
            # df_value.p_to_sql('Fund_EstimatedValue', level2_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df_value.to_sql('Fund_EstimatedValue', level2_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level2/Fund_EstimatedValue')
def Fund_EstimatedValue():
    df_nav_adj, df_share, df_past = read_sql()
    df_value = handle(df_nav_adj, df_share, df_past)
    upload(df_value, df_past)
