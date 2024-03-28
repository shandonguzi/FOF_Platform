
import pandas as pd
from utils.frequent_dates import *
from settings.database import level1_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Stock_Return_m = get_sql(level1_csmar, 'TRD_Mnth')
    
    return Stock_Return_m


def handle(Stock_Return_m):

    ST_Rev = Stock_Return_m[['Stkcd', 'Date', 'RealPctChange']].rename(columns={'RealPctChange': 'ST_Rev'})
    ST_Rev['Date'] = ST_Rev['Date'] + pd.offsets.MonthBegin(1)

    return ST_Rev


def upload(ST_Rev):
    ST_Rev.to_sql('ST_Rev', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ST_Rev')
def ST_Rev():
    Stock_Return_m = read_sql()
    ST_Rev = handle(Stock_Return_m)
    upload(ST_Rev)