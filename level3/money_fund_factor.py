
import pandas as pd

from settings.database import level0_joinquant, level1_wind, level2_csmar, level3_factors
from utils.factor_research.truncate_funcs import (middle_truncate_factor,
                                                  simple_truncate_factor,
                                                  truncate_factor)
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    money_symbol = Classification[Classification.underlying_asset_type == '货币型'].Symbol.values
    EstimatedValue_m = get_sql(level2_csmar, f'select * from Fund_EstimatedValue_m where Symbol in {tuple(money_symbol)}')
    return_money_fund_m = get_sql(level1_wind, 'MoneyFundReturn_m')
    return EstimatedValue_m, return_money_fund_m


def handle(EstimatedValue_m, return_money_fund_m):
    EstimatedValue_m['Date'] = EstimatedValue_m.Date + pd.offsets.MonthBegin(0)
    money_fund_factor = EstimatedValue_m[EstimatedValue_m.Symbol.isin(return_money_fund_m.Symbol)]
    money_fund_factor = money_fund_factor.pivot(index='Date', columns='Symbol', values='EstimatedValue_m')
    money_fund_factor = money_fund_factor.stack().loc[simple_truncate_factor(money_fund_factor)]
    money_fund_factor.name = 'EstimatedValue_m'
    return money_fund_factor

def upload(money_fund_factor):
    money_fund_factor.to_sql('money_fund_factor', con=level3_factors, if_exists='replace')

@timeit('level3/money_fund_factor')
def money_fund_factor():
    EstimatedValue_m, return_money_fund_m = read_sql()
    money_fund_factor = handle(EstimatedValue_m, return_money_fund_m)
    upload(money_fund_factor)
