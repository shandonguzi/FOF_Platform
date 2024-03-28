
import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'BND_Exchange')
    return df

def handle(df):
    df = df.rename(columns={'Clsdt': 'Date', 'Nrrdata': 'riskfree', 'Nrrdaydt': 'riskfree_d'})
    df['Date'] = pd.to_datetime(df.Date)
    return df

def upload(df):
    df.to_sql('BND_Exchange', con=level1_csmar, if_exists='replace', index=False)

@timeit('level1/债券/BND_Exchange')
def BND_Exchange():
    df = read_sql()
    df = handle(df)
    upload(df)
