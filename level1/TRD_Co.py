
import pandas as pd

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'TRD_Co')
    return df

def handle(df):
    df['Listdt'] = pd.to_datetime(df.Listdt)
    return df.set_index('Stkcd')

def upload(df):
    df.to_sql('TRD_Co', con=level1_csmar, if_exists='replace')

@timeit('level1/公司/TRD_Co')
def TRD_Co():
    df = read_sql()
    df = handle(df)
    upload(df)
