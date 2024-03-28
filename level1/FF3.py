
import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'STK_MKT_THRFACDAY')
    return df

def handle(df):
    # 沪深A股
    df = df[df.MarkettypeID == 'P9706'].drop('MarkettypeID', axis=1)
    df = df.rename(columns={'TradingDate': 'Date', 'RiskPremium1': 'MKT', 'SMB1': 'SMB', 'HML1': 'HML'})
    df['Date'] = pd.to_datetime(df.Date)
    return df

def upload(df):
    df.to_sql('FF3', con=level1_csmar, if_exists='replace', index=False)

@timeit('level1/股票/FF3')
def FF3():
    df = read_sql()
    df = handle(df)
    upload(df)