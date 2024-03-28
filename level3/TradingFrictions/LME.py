
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

    Stock_Return_m = Stock_Return_m[['Stkcd', 'Date', 'MKTValue']]
    # Stock_Return_this_m = Stock_Return_this_m[['Stkcd', 'Date', 'MKTValue']]
    # lme = pd.concat([Stock_Return_m, Stock_Return_this_m], axis=0).reset_index(drop=True)
    lme = Stock_Return_m.copy()
    lme = lme.rename(columns={'MKTValue': 'LME'}).dropna()

    return lme


def upload(lme):
    lme.to_sql('LME', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/LME')
def LME():
    Stock_Return_m, Stock_Return_this_m = read_sql()
    lme = handle(Stock_Return_m, Stock_Return_this_m)
    upload(lme)