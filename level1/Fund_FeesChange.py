'''For get fund main info'''

import re

import numpy as np
import pandas as pd
from sqlalchemy.types import VARCHAR

from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_csmar, 'Fund_FeesChange')
    try:
        df_past = get_sql(level1_csmar, 'Fund_FeesChange', index_cols=['Date', 'Symbol', 'NameOfFee'])
        return df, df_past
    except:
        return df, np.nan

def check_汉字(str):
    '''if it is Chinese character'''
    return bool(re.findall(r'([\u4e00-\u9fa5]+)', str))

def check_character(str):
    '''if it is English character'''
    return bool(re.findall(r'([a-zA-Z]+)', str))

def handle(df, df_past):
    '''handle df'''
    df = df[df.NameOfFee.isin(['日常申购费率', '日常赎回费率', '管理费率', '托管费率'])]
    df = df.rename(columns={'DeclareDate': 'Date'})
    df['Date'] = pd.to_datetime(df.Date)
    df.loc[df.ProportionOfFee.apply(lambda x: check_汉字(str(x))), 'ProportionOfFee'] = 1.0
    df.loc[df.ProportionOfFee.apply(lambda x: check_character(str(x))), 'ProportionOfFee'] = 1.0
    df['ProportionOfFee'] = df.ProportionOfFee.str.strip('%')
    df['ProportionOfFee'] = df.ProportionOfFee.str.strip('？')
    df['ProportionOfFee'] = df.ProportionOfFee.replace('NULL', np.nan).astype(float)
    df = df[df.ProportionOfFee < 5]
    df = df.drop_duplicates(['Symbol', 'Date', 'NameOfFee'])
    df = df.set_index(['Date', 'Symbol', 'NameOfFee'])['ProportionOfFee']
    if type(df) == float:
        return df
    else:
        df = pd.concat([df, df_past]).drop_duplicates(keep=False)
        return df

def upload(df):
    df.to_sql('Fund_FeesChange', con=level1_csmar, dtype={'NameOfFee': VARCHAR(length=6)}, if_exists='append')

@timeit('level1/费率变动/Fund_FeesChange')
def Fund_FeesChange():
    df, df_past = read_sql()
    df = handle(df, df_past)
    upload(df)
