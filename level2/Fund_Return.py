
'''For get fund main info'''

import time

import numpy as np
import pandas as pd

import utils.mysql.p_to_sql
from settings.database import level1_csmar, level1_wind, level2_csmar
from utils.frequent_dates import last_month_begin_s
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_nav = get_sql(level1_csmar, 'Fund_NAV')
    df_resolution = get_sql(level1_csmar, 'Fund_Resolution')
    df_dividend = get_sql(level1_csmar, 'Fund_FundDividend')
    money_fund_return = get_sql(level1_wind, 'MoneyFundReturn', index_cols=['Date', 'Symbol'])
    try:
        df_past = get_sql(level2_csmar, 'Fund_Return')
        return df_nav, df_resolution, df_dividend, df_past, money_fund_return
    except:
        return df_nav, df_resolution, df_dividend, np.nan, money_fund_return

def handle(df_return, df_resolution, df_dividend, df_past, money_fund_return):
    df_return = pd.merge(df_return, df_dividend, on=['Date', 'Symbol'], how='left').set_index(['Symbol', 'Date'])
    df_return = pd.merge(df_return, df_resolution, on=['Date', 'Symbol'], how='left').set_index(['Symbol', 'Date'])
    df_return['DividendperShare'] = df_return.DividendperShare.fillna(0)
    df_return['SplitRatio'] = df_return.SplitRatio.fillna(1)
    df_return = df_return.sort_index()
    df_return['NAV_shift'] = df_return.groupby(level=0).apply(lambda x: x.NAV.shift(1)).droplevel(0)
    df_return = df_return.swaplevel()
    df_return = df_return.dropna()
    df_return = (df_return.NAV + df_return.DividendperShare) * df_return.SplitRatio / df_return.NAV_shift - 1
    money_fund_return = money_fund_return[~ money_fund_return.index.duplicated(keep='first')]
    df_return = df_return[~ df_return.index.duplicated(keep='first')]
    co_index = np.intersect1d(money_fund_return.index, df_return.index)
    df_return.loc[co_index] = money_fund_return.loc[co_index]
    df_return.name = 'Return'
    df_return = df_return.replace([np.inf, -np.inf], np.nan).dropna()
    df_return = df_return[df_return < 200]
    df_return = df_return.reset_index()
    if type(df_past) == float:
        return df_return
    else:
        df_return = pd.concat([df_return, df_past]).drop_duplicates(keep=False)
        return df_return

def upload(df_return, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Fund_Return')
        df_return.p_to_sql('Fund_Return', level2_csmar, partitions=1000, n_workers=12, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(df_return) != 0:
            # df_return.p_to_sql('Fund_Return', level2_csmar, partitions=1, n_workers=1, threads_per_worker=1)
            df_return.to_sql('Fund_Return', level2_csmar, if_exists='append', index=False)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level2/Fund_Return')
def Fund_Return():
    df_return, df_resolution, df_dividend, df_past, money_fund_return = read_sql()
    df_return = handle(df_return, df_resolution, df_dividend, df_past, money_fund_return)
    upload(df_return, df_past)
