
'''For get fund main info'''

import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'Fund_Resolution')
    return df

def handle(df):
    df['DeclareDate'] = pd.to_datetime(df.DeclareDate)
    df = df.rename(columns={'DeclareDate': 'Date'})
    df['SplitRatio'] = df.SplitRatio.fillna(1)
    df = df[df.SplitRatio < 5]
    df = df.set_index(['Date', 'Symbol'])
    return df

def upload(df):
    df.to_sql('Fund_Resolution', con=level1_csmar, if_exists='replace')

@timeit('level1/基金拆分信息/Fund_Resolution')
def Fund_Resolution():
    df = read_sql()
    df = handle(df)
    upload(df)
