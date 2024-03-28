
import time

import numpy as np
import pandas as pd

from settings.database import level0_wind, level1_wind
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_nav_adj = get_sql(level0_wind, 'MoneyFundAdjNAV')
    try:
        df_past = get_sql(level1_wind, 'MoneyFundReturn')
        return df_nav_adj, df_past
    except:
        return df_nav_adj, np.nan

def handle(df_nav_adj, df_past):
    MoneyFundReturn = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').NAV_adj.pct_change().dropna())
    MoneyFundReturn = MoneyFundReturn.swaplevel()
    MoneyFundReturn = MoneyFundReturn.replace([np.inf, -np.inf, 0], np.nan).dropna()
    MoneyFundReturn.name = 'MoneyFundReturn'
    MoneyFundReturn = MoneyFundReturn.reset_index()
    if type(df_past) == float:
        return MoneyFundReturn
    else:
        MoneyFundReturn = pd.concat([MoneyFundReturn, df_past]).drop_duplicates(keep=False)
        return MoneyFundReturn

def upload(MoneyFundReturn, df_past):

    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Wind 货币型基金回报率/MoneyFundReturn')
        MoneyFundReturn.p_to_sql('MoneyFundReturn', level1_wind, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Symbol'])
    else:
        if len(MoneyFundReturn) != 0:
            MoneyFundReturn.set_index(['Date', 'Symbol']).to_sql('MoneyFundReturn', con=level1_wind, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')


@timeit('level1/Wind 货币型基金回报率/MoneyFundReturn')
def MoneyFundReturn():
    df_nav_adj, df_past = read_sql()
    MoneyFundReturn = handle(df_nav_adj, df_past)
    upload(MoneyFundReturn, df_past)
