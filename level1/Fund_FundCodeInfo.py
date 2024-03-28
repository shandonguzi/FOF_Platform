'''For get fund main info'''

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'FUND_FundCodeInfo')
    return df

def handle(df):
    '''handle df'''
    df = df.drop_duplicates(['MasterFundCode', 'Symbol'])[['MasterFundCode', 'Symbol']]
    return df

def upload(df):
    df.to_sql('Fund_FundCodeInfo', con=level1_csmar, index=False, if_exists='replace')

@timeit('level1/基金代码信息表/Fund_FundCodeInfo')
def Fund_FundCodeInfo():
    df = read_sql()
    df = handle(df)
    upload(df)
