
import numpy as np

from settings.database import level1_wind, level2_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_nav_adj = get_sql(level2_csmar, 'Fund_NAV_adj')
    money_fund_return_m = get_sql(level1_wind, 'MoneyFundReturn_m', index_cols=['Date', 'Symbol'])
    return df_nav_adj, money_fund_return_m

def handle(df_nav_adj, money_fund_return_m):

    # ed2ed
    fund_return_m = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').last().NAV_adj.pct_change().dropna()).swaplevel()
    fund_return_m = fund_return_m.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    co_index = np.intersect1d(money_fund_return_m.index, fund_return_m.index)
    fund_return_m.loc[co_index] = money_fund_return_m.loc[co_index]
    fund_return_m.name = 'Fund_Return_m'

    # bg2ed
    df_nav_adj_bg = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').first()).NAV_adj
    df_nav_adj_ed = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').last()).NAV_adj
    fund_return_m_bg2ed = (df_nav_adj_ed.dropna() / df_nav_adj_bg.dropna()).swaplevel() - 1
    co_index_bg2ed = np.intersect1d(money_fund_return_m.index, fund_return_m_bg2ed.index)
    fund_return_m_bg2ed.loc[co_index_bg2ed] = money_fund_return_m.loc[co_index_bg2ed]
    fund_return_m_bg2ed = fund_return_m_bg2ed.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    fund_return_m_bg2ed.name = 'Fund_Return_m_bg2ed'

    # mid2mid
    def get_mid(x):
        if len(x) != 0:
            mid = x.iloc[len(x) // 2]
        else:
            mid = np.nan
        return mid

    df_nav_adj_last_mid = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').apply(get_mid)).NAV_adj.shift(1)
    df_nav_adj_this_mid = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').apply(get_mid)).NAV_adj
    fund_return_m_mid2mid = (df_nav_adj_this_mid.dropna() / df_nav_adj_last_mid.dropna()).swaplevel() - 1

    co_index_mid2mid = np.intersect1d(money_fund_return_m.index, fund_return_m_mid2mid.index)
    fund_return_m_mid2mid.loc[co_index_mid2mid] = money_fund_return_m.loc[co_index_mid2mid]
    fund_return_m_mid2mid = fund_return_m_mid2mid.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    fund_return_m_mid2mid.name = 'Fund_Return_m_mid2mid'

    # sec2bg
    def get_second(x):
        if len(x) > 1:
            mid = x.iloc[1]
        else:
            mid = np.nan
        return mid

    df_nav_adj_this_second = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').apply(get_second)).NAV_adj
    df_nav_adj_next_bg = df_nav_adj.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').first()).NAV_adj.shift(-1)
    fund_return_m_sec2bg = (df_nav_adj_next_bg.dropna() / df_nav_adj_this_second.dropna()).swaplevel() - 1

    co_index_sec2bg = np.intersect1d(money_fund_return_m.index, fund_return_m_sec2bg.index)
    fund_return_m_sec2bg.loc[co_index_sec2bg] = money_fund_return_m.loc[co_index_sec2bg]
    fund_return_m_sec2bg = fund_return_m_sec2bg.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    fund_return_m_sec2bg.name = 'Fund_Return_m_sec2bg'

    return fund_return_m, fund_return_m_bg2ed, fund_return_m_mid2mid, fund_return_m_sec2bg

def upload(fund_return_m, fund_return_m_bg2ed, fund_return_m_mid2mid, fund_return_m_sec2bg):
    fund_return_m.to_sql('Fund_Return_m', con=level2_csmar, if_exists='replace')
    fund_return_m_bg2ed.to_sql('Fund_Return_m_bg2ed', con=level2_csmar, if_exists='replace')
    fund_return_m_mid2mid.to_sql('Fund_Return_m_mid2mid', con=level2_csmar, if_exists='replace')
    fund_return_m_sec2bg.to_sql('Fund_Return_m_sec2bg', con=level2_csmar, if_exists='replace')

@timeit('level2/Fund_Return_m')
def Fund_Return_m():
    df_nav_adj, money_fund_return_m = read_sql()
    fund_return_m, fund_return_m_bg2ed, fund_return_m_mid2mid, fund_return_m_sec2bg = handle(df_nav_adj, money_fund_return_m)
    upload(fund_return_m, fund_return_m_bg2ed, fund_return_m_mid2mid, fund_return_m_sec2bg)
