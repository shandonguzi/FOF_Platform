
import pandas as pd
import numpy as np

from settings.database import level0_joinquant, level3_factors, level4_factor_result
from utils.factor_research.select_top_underlying import select_top_underlying
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    risky_fund_factor = get_sql(level3_factors, 'z_score', index='Date', columns='Symbol', values='z_score')
    risky_fund_factor = risky_fund_factor[risky_fund_factor.index <= pd.to_datetime('2024-01-01')]
    risky_fund_factor2 = get_sql(level3_factors, 'ipca_score', index='Date', columns='Symbol', values='ipca_score')
    risky_fund_factor2 = risky_fund_factor2[risky_fund_factor2.index >= pd.to_datetime('2024-02-01')]
    risky_fund_factor = pd.concat([risky_fund_factor, risky_fund_factor2], axis=0)
    # risky_fund_factor = get_sql(level3_factors, 'ipca_score', index='Date', columns='Symbol', values='ipca_score')
    bond_fund_factor = get_sql(level3_factors, 'TD_α', index='Date', columns='Symbol', values='TD_α')
    money_fund_factor = get_sql(level3_factors, 'money_fund_factor', index='Date', columns='Symbol', values='EstimatedValue_m')
    
    funds_name = get_sql(level0_joinquant, 'Fund_MainInfo', index_cols='Symbol').name
    underlying_asset_type = get_sql(level0_joinquant, 'Fund_MainInfo', index_cols='Symbol').underlying_asset_type
    return risky_fund_factor, bond_fund_factor, money_fund_factor, funds_name, underlying_asset_type

def handle(risky_fund_factor, bond_fund_factor, money_fund_factor, funds_name, underlying_asset_type, top_num):

    risky_funds = select_top_underlying(risky_fund_factor, top_num).add_prefix('Risky Fund ')
    bond_funds = select_top_underlying(bond_fund_factor, top_num).add_prefix('Bond Fund ')
    money_funds = select_top_underlying(money_fund_factor, top_num).add_prefix('Money Fund ')

    top_funds = pd.concat([risky_funds, bond_funds, money_funds], axis=1)
    funds_name[np.nan] = np.nan
    top_funds_name = top_funds.applymap(lambda x: funds_name.loc[x])
    underlying_asset_type[np.nan] = np.nan
    top_funds_type = top_funds.applymap(lambda x: underlying_asset_type.loc[x])

    return top_funds, top_funds_name, top_funds_type

def upload(top_funds, top_funds_name, top_funds_type):
    top_funds.to_sql('top_funds', con=level4_factor_result, if_exists='replace')
    top_funds_name.to_sql('top_funds_name', con=level4_factor_result, if_exists='replace')
    top_funds_type.to_sql('top_funds_type', con=level4_factor_result, if_exists='replace')

@timeit('level4/top_funds')
def top_funds(top_num):
    risky_fund_factor, bond_fund_factor, money_fund_factor, funds_name, underlying_asset_type = read_sql()
    top_funds, top_funds_name, top_funds_type = handle(risky_fund_factor, bond_fund_factor, money_fund_factor, funds_name, underlying_asset_type, top_num)
    upload(top_funds, top_funds_name, top_funds_type)
