
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Stock_Return_d = get_sql(level1_csmar, 'TRD_Dalyr')
    
    return Stock_Return_d


def handle(Stock_Return_d):

    Stock_Return_d = Stock_Return_d[['Stkcd', 'Date']]
    Stock_last_day = Stock_Return_d.groupby(['Stkcd']).last().reset_index()
    Stock_last_day.rename(columns={'Date': 'LastDay'}, inplace=True)

    return Stock_last_day


def upload(Stock_last_day):
    Stock_last_day.to_sql('Stock_last_day', con=level2_csmar, if_exists='replace', index=False)


@timeit('level2/Stock_last_day')
def Stock_last_day():
    Stock_Return_d = read_sql()
    Stock_last_day = handle(Stock_Return_d)
    upload(Stock_last_day)