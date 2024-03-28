
'''For get fund main info'''

import time

import numpy as np
import pandas as pd
import sys
sys.path.append('/code')
import utils.mysql.p_to_sql
from settings.database import level1_csmar, level2_csmar
from utils.frequent_dates import last_month_begin_s
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_nav = get_sql(level1_csmar, 'Fund_NAV')
    df_resolution = get_sql(level1_csmar, 'Fund_Resolution')
    df_dividend = get_sql(level1_csmar, 'Fund_FundDividend')
    try:
        df_past = get_sql(level2_csmar, 'Fund_NAV_adj')
        return df_nav, df_resolution, df_dividend, df_past
    except:
        return df_nav, df_resolution, df_dividend, np.nan

def handle(df_nav, df_resolution, df_dividend, df_past):
    df_nav = pd.merge(df_nav, df_dividend, on=['Date', 'Symbol'], how='left').set_index(['Symbol', 'Date'])
    df_nav = pd.merge(df_nav, df_resolution, on=['Date', 'Symbol'], how='left').set_index(['Symbol', 'Date'])
    df_nav['DividendperShare'] = df_nav.DividendperShare.fillna(0)
    df_nav['SplitRatio'] = df_nav.SplitRatio.fillna(1)
    df_nav = df_nav.sort_index()
    df_nav['NAV_shift'] = df_nav.groupby(level=0).apply(lambda x: x.NAV.shift(1)).droplevel(0)
    df_nav = df_nav.swaplevel()
    df_nav = df_nav.dropna()
    df_nav = df_nav.NAV * (((df_nav.NAV_shift * df_nav.SplitRatio) / (df_nav.NAV_shift - df_nav.DividendperShare)).groupby('Symbol').cumprod())
    df_nav.name = 'NAV_adj'
    df_nav = df_nav.replace([np.inf, -np.inf], np.nan).dropna()
    df_nav = df_nav[df_nav < 200]
    df_nav = df_nav.reset_index()
    if type(df_past) == float:
        return df_nav
    else:
        df_nav = pd.concat([df_nav, df_past]).drop_duplicates(keep=False)
        return df_nav

def upload(df_nav, df_past):

    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Fund_NAV_adj')
        df_nav.p_to_sql('Fund_NAV_adj', level2_csmar, partitions=1000, n_workers=12, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df_nav) != 0:
            # df_nav.p_to_sql('Fund_NAV_adj', level2_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df_nav.to_sql('Fund_NAV_adj', level2_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level2/Fund_NAV_adj')
def Fund_NAV_adj():
    df_nav, df_resolution, df_dividend, df_past = read_sql()
    df_nav = handle(df_nav, df_resolution, df_dividend, df_past)
    upload(df_nav, df_past)
