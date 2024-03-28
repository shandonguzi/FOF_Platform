
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

    Fund_EstimatedValue_m = get_sql(level2_csmar, 'Fund_EstimatedValue_m')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    
    return Fund_EstimatedValue_m, Classification


def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def handle(Fund_EstimatedValue_m, Classification):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)

    Fund_tna = Fund_EstimatedValue_m.copy()
    Fund_tna['Date'] = Fund_tna['Date'] - pd.offsets.MonthBegin(1)
    Fund_tna = Fund_tna.rename(columns={'EstimatedValue_m': 'TNA'})
    Fund_tna = Fund_tna[Fund_tna.Symbol.isin(filtered_risky_symbol)]

    return Fund_tna


def upload(Fund_tna):
    Fund_tna.to_sql('Fund_tna', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Fund_TNA')
def Fund_TNA():
    Fund_EstimatedValue_m, Classification = read_sql()
    Fund_tna = handle(Fund_EstimatedValue_m, Classification)
    upload(Fund_tna)