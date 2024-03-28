
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')
    
    return TRD_Dalyr


def handle(TRD_Dalyr):
    TRD_Dalyr = TRD_Dalyr[['Stkcd', 'Date', 'Close']].sort_values(['Stkcd', 'Date'])
    TRD_Dalyr['High'] = TRD_Dalyr.set_index('Date').groupby('Stkcd').rolling(252).max()['Close'].values
    
    rel2high = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('M').last().drop('Stkcd', axis=1).reset_index()
    rel2high['Lm'] = rel2high.groupby('Stkcd')['Close'].shift(1)
    rel2high['Rel2High'] = rel2high['Lm'] / rel2high['High']
    rel2high = rel2high.drop(['Close', 'High', 'Lm'], axis=1).dropna()
    rel2high['Date'] = rel2high['Date'] - pd.offsets.MonthBegin(1)

    return rel2high


def upload(rel2high):
    rel2high.to_sql('Rel2High', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Rel2High')
def Rel2High():
    TRD_Dalyr = read_sql()
    rel2high = handle(TRD_Dalyr)
    upload(rel2high)