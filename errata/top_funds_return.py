
import numpy as np
import pandas as pd

from settings.database import level2_csmar, errata_jiayin_robo_advisor
from utils.frequent_dates import next_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    top_funds = get_sql(errata_jiayin_robo_advisor, 'top_funds', index_cols='Date')
    top_funds = top_funds.applymap(lambda x: int(x.split('.')[0]))
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index='Date', columns='Symbol', values='Fund_Return_m')
    # Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m_sec2bg', index='Date', columns='Symbol', values='Fund_Return_m_sec2bg')

    return top_funds, Fund_Return_m

def get_top_underlying_return(date, funds, ranks, return_matrix):
    
    if date == next_month_end:
        underlying_return = pd.Series([np.nan] * len(ranks))
    else:
        try:
            underlying_return = return_matrix.loc[date, funds]
        except:
            # 月初是非交易日时，在月初将取不到当月最后一天基金的复权净值，在这种情况下赋值为NA
            underlying_return = pd.Series([np.nan] * len(ranks))
    underlying_return.index = ranks
    return underlying_return

def handle(top_funds, Fund_Return_m):
    Fund_Return_m.replace(np.nan, 0, inplace=True)
    top_funds_return = top_funds.apply(lambda x: get_top_underlying_return(x.name + pd.offsets.MonthEnd(0), x.values, x.index, Fund_Return_m), axis=1)
    return top_funds_return

def upload(top_funds_return):
    top_funds_return.to_sql('top_funds_return', con=errata_jiayin_robo_advisor, if_exists='replace')

@timeit('errata/top_funds_return')
def top_funds_return():
    top_funds, Fund_Return_m = read_sql()
    top_funds_return = handle(top_funds, Fund_Return_m)
    upload(top_funds_return)
