'''For get fund main info'''

import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'Fund_FundDividend')
    return df

def handle(df):
    df['SecondaryExDividendDate'] = pd.to_datetime(df.SecondaryExDividendDate)
    df['PrimaryExDividendDate'] = pd.to_datetime(df.PrimaryExDividendDate)
    ExDividendMonth = df.PrimaryExDividendDate.copy()
    ExDividendMonth.name = 'ExDividendMonth'
    ExDividendMonth[ExDividendMonth.isnull()] = df.SecondaryExDividendDate[ExDividendMonth.isnull()].copy()
    df['ExDividendMonth'] = ExDividendMonth
    df = df[['Symbol', 'ExDividendMonth', 'DividendperShare']]
    df = df.rename(columns={'ExDividendMonth': 'Date'})
    df['DividendperShare'] = df.DividendperShare.fillna(0)
    df = df.groupby(['Date', 'Symbol']).sum()
    df = df[df.DividendperShare < 1]
    return df

def upload(df):
    df.to_sql('Fund_FundDividend', con=level1_csmar, if_exists='replace')

@timeit('level1/基金分配/Fund_FundDividend')
def Fund_FundDividend():
    df = read_sql()
    df = handle(df)
    upload(df)
