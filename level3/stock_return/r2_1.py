
import pandas as pd
from utils.frequent_dates import *
from settings.database import level1_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Stock_Return_m = get_sql(level1_csmar, 'TRD_Mnth')
    
    return Stock_Return_m


def handle(Stock_Return_m):

    r2_1 = Stock_Return_m[['Date', 'Stkcd', 'RealPctChange']].rename(columns={'RealPctChange': 'r2_1'})
    r2_1['Date'] = r2_1['Date'] + pd.offsets.MonthBegin(1)

    return r2_1


def upload(r2_1):
    r2_1.to_sql('r2_1', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/r2_1')
def r2_1():
    Stock_Return_m = read_sql()
    r2_1 = handle(Stock_Return_m)
    upload(r2_1)