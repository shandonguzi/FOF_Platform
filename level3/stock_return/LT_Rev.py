
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
    df['LT_Rev'] = (df['lagged'] + 1).rolling(lag2).apply(np.prod, raw=True) - 1
    return df


def handle(Stock_Return_m, Stock_Return_this_m):

    Stock_Return_m = Stock_Return_m[['Stkcd', 'Date', 'RealPctChange']]
    # Stock_Return_this_m = Stock_Return_this_m[['Stkcd', 'Date', 'RealPctChange']]
    # Stock_Return_m = pd.concat([Stock_Return_m, Stock_Return_this_m], axis=0).reset_index(drop=True)
    LT_Rev = Stock_Return_m.groupby('Stkcd').apply(lambda x: calculate_cumulative_return(x, 13, 47))
    LT_Rev = LT_Rev.reset_index(drop=True).drop(['RealPctChange', 'lagged'], axis=1).dropna()

    return LT_Rev


def upload(LT_Rev):
    LT_Rev.to_sql('LT_Rev', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/LT_Rev')
def LT_Rev():
    Stock_Return_m, Stock_Return_this_m = read_sql()
    LT_Rev = handle(Stock_Return_m, Stock_Return_this_m)
    upload(LT_Rev)