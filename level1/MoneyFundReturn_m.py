'''For get fund main info'''

import numpy as np

from settings.database import level0_wind, level1_wind
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df_nav_adj = get_sql(level0_wind, 'MoneyFundAdjNAV')
    return df_nav_adj

def handle(df_nav_adj):
    MoneyFundReturn_m = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').last().NAV_adj.pct_change().dropna())
    MoneyFundReturn_m = MoneyFundReturn_m.swaplevel()
    MoneyFundReturn_m = MoneyFundReturn_m.replace([np.inf, -np.inf, 0], np.nan).dropna()
    MoneyFundReturn_m.name = 'MoneyFundReturn_m'
    return MoneyFundReturn_m

def upload(MoneyFundReturn_m):
    MoneyFundReturn_m.to_sql('MoneyFundReturn_m', con=level1_wind, if_exists='replace')

@timeit('level1/Wind 货币型基金月度回报率/MoneyFundReturn_m')
def MoneyFundReturn_m():
    df_nav_adj = read_sql()
    MoneyFundReturn_m = handle(df_nav_adj)
    upload(MoneyFundReturn_m)
