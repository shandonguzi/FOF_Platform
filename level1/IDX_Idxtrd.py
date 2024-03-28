
'''For get fund main info'''

import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    sz50 = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '000016'")
    hs300 = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '000300'")
    zz500 = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '000905'")
    zz1000 = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '000852'")
    sz = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '000001'")
    zz2000 = get_sql(level0_csmar, "select * from IDX_Idxtrd where Indexcd = '932000'")

    return sz50, hs300, zz500, zz1000, sz, zz2000

def handle(sz50, hs300, zz500, zz1000, sz, zz2000):
    '''handle df'''
    sz50['Idxtrd01'] = pd.to_datetime(sz50.Idxtrd01)
    hs300['Idxtrd01'] = pd.to_datetime(hs300.Idxtrd01)
    zz500['Idxtrd01'] = pd.to_datetime(zz500.Idxtrd01)
    zz1000['Idxtrd01'] = pd.to_datetime(zz1000.Idxtrd01)
    sz['Idxtrd01'] = pd.to_datetime(sz.Idxtrd01)
    zz2000['Idxtrd01'] = pd.to_datetime(zz2000.Idxtrd01)

    sz50 = sz50.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})
    hs300 = hs300.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})
    zz500 = zz500.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})
    zz1000 = zz1000.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})
    sz = sz.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})
    zz2000 = zz2000.rename(columns={'Idxtrd01': 'Date', 'Idxtrd05': 'Close'})

    sz50 = sz50[['Date', 'Close']].drop_duplicates()
    hs300 = hs300[['Date', 'Close']].drop_duplicates()
    zz500 = zz500[['Date', 'Close']].drop_duplicates()
    zz1000 = zz1000[['Date', 'Close']].drop_duplicates()
    sz = sz[['Date', 'Close']].drop_duplicates()
    zz2000 = zz2000[['Date', 'Close']].drop_duplicates()

    sz50 = sz50.set_index(['Date']).resample('d').ffill()
    hs300 = hs300.set_index(['Date']).resample('d').ffill()
    zz500 = zz500.set_index(['Date']).resample('d').ffill()
    zz1000 = zz1000.set_index(['Date']).resample('d').ffill()
    sz = sz.set_index(['Date']).resample('d').ffill()
    zz2000 = zz2000.set_index(['Date']).resample('d').ffill()

    sz50_daily = sz50.pct_change().dropna()
    hs300_daily = hs300.pct_change().dropna()
    zz500_daily = zz500.pct_change().dropna()
    zz1000_daily = zz1000.pct_change().dropna()
    sz_daily = sz.pct_change().dropna()
    zz2000_daily = zz2000.pct_change().dropna()

    sz50_monthly_ed2ed = sz50.resample('M').last().pct_change()
    sz50_monthly_ed2ed = sz50_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})
    hs300_monthly_ed2ed = hs300.resample('M').last().pct_change()
    hs300_monthly_ed2ed = hs300_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})
    zz500_monthly_ed2ed = zz500.resample('M').last().pct_change()
    zz500_monthly_ed2ed = zz500_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})
    zz1000_monthly_ed2ed = zz1000.resample('M').last().pct_change()
    zz1000_monthly_ed2ed = zz1000_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})
    sz_monthly_ed2ed = sz.resample('M').last().pct_change()
    sz_monthly_ed2ed = sz_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})
    zz2000_monthly_ed2ed = zz2000.resample('M').last().pct_change()
    zz2000_monthly_ed2ed = zz2000_monthly_ed2ed.rename(columns={'Close': 'ed2ed'})

    sz50_monthly_bg2ed = sz50.resample('M').last() / sz50.resample('M').first() - 1
    sz50_monthly_bg2ed = sz50_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})
    hs300_monthly_bg2ed = hs300.resample('M').last() / hs300.resample('M').first() - 1
    hs300_monthly_bg2ed = hs300_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})
    zz500_monthly_bg2ed = zz500.resample('M').last() / zz500.resample('M').first() - 1
    zz500_monthly_bg2ed = zz500_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})
    zz1000_monthly_bg2ed = zz1000.resample('M').last() / zz1000.resample('M').first() - 1
    zz1000_monthly_bg2ed = zz1000_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})
    sz_monthly_bg2ed = sz.resample('M').last() / sz.resample('M').first() - 1
    sz_monthly_bg2ed = sz_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})
    zz2000_monthly_bg2ed = zz2000.resample('M').last() / zz2000.resample('M').first() - 1
    zz2000_monthly_bg2ed = zz2000_monthly_bg2ed.rename(columns={'Close': 'bg2ed'})

    sz50_monthly = pd.concat([sz50_monthly_ed2ed, sz50_monthly_bg2ed], axis=1).dropna()
    hs300_monthly = pd.concat([hs300_monthly_ed2ed, hs300_monthly_bg2ed], axis=1).dropna()
    zz500_monthly = pd.concat([zz500_monthly_ed2ed, zz500_monthly_bg2ed], axis=1).dropna()
    zz1000_monthly = pd.concat([zz1000_monthly_ed2ed, zz1000_monthly_bg2ed], axis=1).dropna()
    sz_monthly = pd.concat([sz_monthly_ed2ed, sz_monthly_bg2ed], axis=1).dropna()
    zz2000_monthly = pd.concat([zz2000_monthly_ed2ed, zz2000_monthly_bg2ed], axis=1).dropna()

    return sz50, hs300, zz500, zz1000, sz50_daily, hs300_daily, zz500_daily, zz1000_daily, sz50_monthly, hs300_monthly, zz500_monthly, zz1000_monthly, sz, zz2000, sz_daily, zz2000_daily, sz_monthly, zz2000_monthly

def upload(sz50, hs300, zz500, zz1000, sz50_daily, hs300_daily, zz500_daily, zz1000_daily, sz50_monthly, hs300_monthly, zz500_monthly, zz1000_monthly, sz, zz2000, sz_daily, zz2000_daily, sz_monthly, zz2000_monthly):    
    sz50.to_sql('sz50', con=level1_csmar, if_exists='replace')
    hs300.to_sql('hs300', con=level1_csmar, if_exists='replace')
    zz500.to_sql('zz500', con=level1_csmar, if_exists='replace')
    zz1000.to_sql('zz1000', con=level1_csmar, if_exists='replace')
    sz50_daily.to_sql('sz50_daily', con=level1_csmar, if_exists='replace')
    hs300_daily.to_sql('hs300_daily', con=level1_csmar, if_exists='replace')
    zz500_daily.to_sql('zz500_daily', con=level1_csmar, if_exists='replace')
    zz1000_daily.to_sql('zz1000_daily', con=level1_csmar, if_exists='replace')
    sz50_monthly.to_sql('sz50_monthly', con=level1_csmar, if_exists='replace')
    hs300_monthly.to_sql('hs300_monthly', con=level1_csmar, if_exists='replace')
    zz500_monthly.to_sql('zz500_monthly', con=level1_csmar, if_exists='replace')
    zz1000_monthly.to_sql('zz1000_monthly', con=level1_csmar, if_exists='replace')
    sz.to_sql('sz', con=level1_csmar, if_exists='replace')
    zz2000.to_sql('zz2000', con=level1_csmar, if_exists='replace')
    sz_daily.to_sql('sz_daily', con=level1_csmar, if_exists='replace')
    zz2000_daily.to_sql('zz2000_daily', con=level1_csmar, if_exists='replace')
    sz_monthly.to_sql('sz_monthly', con=level1_csmar, if_exists='replace')
    zz2000_monthly.to_sql('zz2000_monthly', con=level1_csmar, if_exists='replace')


@timeit('level1/国内指数日行情/sz50, hs300, zz500, zz1000, sz, zz2000')
def IDX_Idxtrd():
    sz50, hs300, zz500, zz1000, sz, zz2000 = read_sql()
    sz50, hs300, zz500, zz1000, sz50_daily, hs300_daily, zz500_daily, zz1000_daily, sz50_monthly, hs300_monthly, zz500_monthly, zz1000_monthly, sz, zz2000, sz_daily, zz2000_daily ,sz_monthly, zz2000_monthly = handle(sz50, hs300, zz500, zz1000, sz, zz2000)
    upload(sz50, hs300, zz500, zz1000, sz50_daily, hs300_daily, zz500_daily, zz1000_daily, sz50_monthly, hs300_monthly, zz500_monthly, zz1000_monthly, sz, zz2000, sz_daily, zz2000_daily, sz_monthly, zz2000_monthly)

if __name__ == '__main__':
    IDX_Idxtrd()