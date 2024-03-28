
from functools import reduce

import numpy as np
import pandas as pd
from parallel_pandas import ParallelPandas
from sklearn.linear_model import LinearRegression

from settings.database import level0_jiayin, level0_joinquant, level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

ParallelPandas.initialize(8)

def read_sql():

    Fund_NAV_adj = get_sql(level2_csmar, 'Fund_NAV_adj')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    
    return Fund_NAV_adj, Classification


def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def calculate_monthly_survival(row):

    date_range = pd.date_range(start=row['StartDay'], end=row['LastDay'] + pd.offsets.MonthEnd(0), freq='M')
    date_range = date_range[date_range > row['StartDay']]
    survival_days = np.clip((date_range - row['StartDay']).days, 0, (row['LastDay'] - row['StartDay']).days)

    return pd.DataFrame({
        'Symbol': row['Symbol'],
        'Date': date_range,
        'Age': survival_days
    })


def handle(Fund_NAV_adj, Classification):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)

    Fund_last_day = Fund_NAV_adj.groupby('Symbol').last()['Date'].reset_index().rename(columns={'Date': 'LastDay'})
    Fund_start_day = Classification[['Symbol', 'start_date']].rename(columns={'start_date': 'StartDay'})
    Fund_NAV_adj = pd.merge(Fund_NAV_adj, Fund_start_day, on='Symbol', how='inner')
    Fund_NAV_adj = pd.merge(Fund_NAV_adj, Fund_last_day, on='Symbol', how='inner')
    Fund_day = Fund_NAV_adj[['Symbol', 'StartDay', 'LastDay']].drop_duplicates(subset=['Symbol']).dropna()

    fund_age = pd.concat(Fund_day.apply(calculate_monthly_survival, axis=1).tolist(), ignore_index=True)
    fund_age = fund_age[fund_age.Symbol.isin(filtered_risky_symbol)]
    fund_age['Date'] = fund_age['Date'] - pd.offsets.MonthBegin(1)

    return fund_age


def upload(fund_age):
    fund_age.to_sql('Fund_age', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Fund_age')
def Fund_age():
    Fund_NAV_adj, Classification = read_sql()
    fund_age = handle(Fund_NAV_adj, Classification)
    upload(fund_age)