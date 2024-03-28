
'''For get fund main info'''

import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'Fund_ShareChange')
    return df

def handle(df):
    df['EndDate'] = pd.to_datetime(df.EndDate)
    df = df.rename(columns={'EndDate': 'Date'})
    df = df.drop_duplicates(['Date', 'Symbol'])
    df = df.set_index(['Date', 'Symbol'])
    return df.EndDateShares.sort_index()
    
def upload(df):
    df.to_sql('Fund_ShareChange', con=level1_csmar, if_exists='replace')

@timeit('level1/份额变动/Fund_ShareChange')
def Fund_ShareChange():
    df = read_sql()
    df = handle(df)
    upload(df)
