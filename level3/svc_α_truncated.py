
import numpy as np

from settings.database import level0_joinquant, level2_csmar, level3_factors
from utils.factor_research.truncate_funcs import (middle_truncate_factor,
                                                  simple_truncate_factor,
                                                  truncate_factor)
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    svc_α = get_sql(level3_factors, 'svc_α', index='Date', columns='Symbol', values='svc_α')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')

    return svc_α, Classification

def handle(svc_α, Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)


    #! modify here
    svc_α_truncated = svc_α.stack().loc[middle_truncate_factor(svc_α)]
    # svc_α_truncated = svc_α.stack().loc[simple_truncate_factor(svc_α)]

    # m_return_of_risky_fund = get_sql(level2_csmar, f'select * from Fund_Return_m where Symbol in {tuple(risky_symbol)}', index='Date', columns='Symbol', values='Fund_Return_m')
    # svc_α_truncated = svc_α.stack().loc[truncate_factor(svc_α, m_return_of_risky_fund, simple_truncate_proportion=.00, ivol_truncate_proportion=.00, value_threshold=1e7, return_truncate_proportion=.00)]
    svc_α_truncated.name = 'svc_α_truncated'
    return svc_α_truncated

def upload(svc_α_truncated):
    svc_α_truncated.to_sql('svc_α_truncated', con=level3_factors, if_exists='replace')

@timeit('level3/svc_α_truncated')
def svc_α_truncated():
    svc_α, Classification = read_sql()
    svc_α_truncated = handle(svc_α, Classification)
    upload(svc_α_truncated)
