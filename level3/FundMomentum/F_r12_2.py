
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    svc_α = get_sql(level3_factors, 'svc_α')
    
    return svc_α


def calculate_cumulative_return(df, lag1, lag2, min_window):
    df['lagged'] = df['svc_α'].shift(lag1)
    df = df.dropna()
    df['F_r12_2'] = df['lagged'].rolling(lag2, min_periods=min_window).apply(np.mean, raw=True)
    return df


def handle(svc_α):

    F_r12_2 = svc_α.groupby('Symbol').apply(lambda x: calculate_cumulative_return(x, 2, 10, 8))
    F_r12_2 = F_r12_2.reset_index(drop=True).drop(['svc_α', 'lagged'], axis=1).dropna()
    F_r12_2['Date'] = F_r12_2['Date'] - pd.offsets.MonthBegin(1)

    return F_r12_2


def upload(F_r12_2):
    F_r12_2.to_sql('F_r12_2', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/F_r12_2')
def F_r12_2():
    svc_α = read_sql()
    F_r12_2 = handle(svc_α)
    upload(F_r12_2)