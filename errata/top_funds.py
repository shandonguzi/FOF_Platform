

from settings.database import (errata_jiayin_robo_advisor, level4_factor_result,
                               product_jiayin_robo_advisor)
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from utils.frequent_dates import this_month_begin_s, next_month_begin_s
import pandas as pd

def read_sql():
    previous_top_funds = get_sql(product_jiayin_robo_advisor, 'funds_code', index_cols='Date')
    new_top_funds = get_sql(level4_factor_result, 'top_funds', index_cols='Date')
    previous_top_funds_name = get_sql(product_jiayin_robo_advisor, 'funds_name', index_cols='Date')
    new_top_funds_name = get_sql(level4_factor_result, 'top_funds_name', index_cols='Date')
    return previous_top_funds, new_top_funds, previous_top_funds_name, new_top_funds_name

def handle(previous_top_funds, new_top_funds, previous_top_funds_name, new_top_funds_name):
    top_funds = previous_top_funds.copy()
    funds_name = previous_top_funds_name.copy()
    # top_funds.loc[this_month_begin_s] = new_top_funds.loc[this_month_begin_s].astype(int).astype(str).str.zfill(6) + '.OF'
    # funds_name.loc[this_month_begin_s] = new_top_funds_name.loc[this_month_begin_s]

    try:
        top_funds.loc[next_month_begin_s] = new_top_funds.loc[next_month_begin_s].astype(int).astype(str).str.zfill(6) + '.OF'
        funds_name.loc[next_month_begin_s] = new_top_funds_name.loc[next_month_begin_s]
    except:
        top_funds.loc[this_month_begin_s] = new_top_funds.loc[this_month_begin_s].astype(int).astype(str).str.zfill(6) + '.OF'
        funds_name.loc[this_month_begin_s] = new_top_funds_name.loc[this_month_begin_s]
        # raise ValueError('Next month funds not calculated')
        
    top_funds.index = pd.to_datetime(top_funds.index)
    funds_name.index = pd.to_datetime(funds_name.index)
    return top_funds, funds_name

def upload(top_funds, top_funds_name):
    top_funds.to_sql('top_funds', con=errata_jiayin_robo_advisor, if_exists='replace')
    top_funds_name.to_sql('top_funds_name', con=errata_jiayin_robo_advisor, if_exists='replace')

@timeit('errata/top_funds')
def top_funds():
    previous_top_funds, new_top_funds, previous_top_funds_name, new_top_funds_name = read_sql()
    top_funds, top_funds_name = handle(previous_top_funds, new_top_funds, previous_top_funds_name, new_top_funds_name)
    upload(top_funds, top_funds_name)
