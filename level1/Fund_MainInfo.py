
'''For get fund main info'''

import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'FUND_MainInfo')
    return df

def handle(df):
    df['InceptionDate'] = pd.to_datetime(df.InceptionDate)
    symbol_code_mapping = get_sql(level0_csmar, 'FUND_FundCodeInfo').drop_duplicates(['MasterFundCode', 'Symbol'])[['MasterFundCode', 'Symbol']]
    df = pd.merge(df, symbol_code_mapping, on='MasterFundCode', how='inner')
    df = df.set_index(['Symbol'])[['FullName', 'InceptionDate']]
    return df

def upload(df):
    df.to_sql('Fund_MainInfo', con=level1_csmar, if_exists='replace')

@timeit('level1/基金主体信息表/Fund_MainInfo')
def Fund_MainInfo():
    df = read_sql()
    df = handle(df)
    upload(df)
