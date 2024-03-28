
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


def handle(Stock_Return_m, Stock_Return_this_m):

    Stock_Return_m['TotalShares'] = Stock_Return_m['MKTValue'] / Stock_Return_m['Close']
    Stock_Return_m['AverageVolume'] = Stock_Return_m['TradingVolume'].rolling(3).mean()
    Stock_Return_m = Stock_Return_m[['Stkcd', 'Date', 'AverageVolume', 'TotalShares']]
    # Stock_Return_this_m = Stock_Return_this_m[['Stkcd', 'Date', 'TradingVolume', 'TotalShares']].rename(columns={'TradingVolume': 'AverageVolume'})
    # Stock_Return_m = pd.concat([Stock_Return_m, Stock_Return_this_m], axis=0).reset_index(drop=True)
    Stock_Return_m['LTurnover'] = Stock_Return_m['AverageVolume'] / Stock_Return_m['TotalShares']
    lturnover = Stock_Return_m[['Stkcd', 'Date', 'LTurnover']]
    lturnover = lturnover.dropna()

    return lturnover


def upload(lturnover):
    lturnover.to_sql('LTurnover', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/LTurnover')
def LTurnover():
    Stock_Return_m, Stock_Return_this_m = read_sql()
    lturnover = handle(Stock_Return_m, Stock_Return_this_m)
    upload(lturnover)