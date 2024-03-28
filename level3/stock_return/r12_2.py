
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Stock_Return_m = get_sql(level1_csmar, 'TRD_Mnth')
    Stock_Return_this_m = get_sql(level2_csmar, 'cm_return')
    
    return Stock_Return_m, Stock_Return_this_m


def calculate_cumulative_return(df, lag1, lag2):
    df['lagged'] = df['RealPctChange'].shift(lag1)
    df = df.dropna()
    df['r12_2'] = (df['lagged'] + 1).rolling(lag2).apply(np.prod, raw=True) - 1
    return df


def handle(Stock_Return_m, Stock_Return_this_m):

    Stock_Return_m = Stock_Return_m[['Date', 'Stkcd', 'RealPctChange']]
    # Stock_Return_this_m = Stock_Return_this_m[['Date', 'Stkcd', 'RealPctChange']]
    # Stock_Return_m = pd.concat([Stock_Return_m, Stock_Return_this_m], axis=0).reset_index(drop=True)
    r12_2 = Stock_Return_m.groupby('Stkcd').apply(lambda x: calculate_cumulative_return(x, 2, 10))
    r12_2 = r12_2.reset_index(drop=True).drop(['RealPctChange', 'lagged'], axis=1).dropna()

    return r12_2


def upload(r12_2):
    r12_2.to_sql('r12_2', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/r12_2')
def r12_2():
    Stock_Return_m, Stock_Return_this_m = read_sql()
    r12_2 = handle(Stock_Return_m, Stock_Return_this_m)
    upload(r12_2)