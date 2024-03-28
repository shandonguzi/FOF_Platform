
import pandas as pd
from utils.frequent_dates import *
from settings.database import level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    svc_α = get_sql(level3_factors, 'svc_α')
    
    return svc_α


def handle(svc_α):

    f_st_rev = svc_α.copy()
    f_st_rev = f_st_rev.rename(columns={'svc_α': 'F_ST_Rev'})
    f_st_rev['Date'] = f_st_rev['Date'] - pd.offsets.MonthBegin(1)

    return f_st_rev


def upload(f_st_rev):
    f_st_rev.to_sql('F_ST_Rev', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/F_ST_Rev')
def F_ST_Rev():
    svc_α = read_sql()
    f_st_rev = handle(svc_α)
    upload(f_st_rev)