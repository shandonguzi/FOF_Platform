
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

    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])
    CH3 = get_sql(level0_jiayin, 'm_CH3', index_cols='Date')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    
    return Fund_Return_m, CH3, Classification


def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def get_Fund_Allocation_filter(filtered_risky_symbol):

    Fund_Allocation = get_sql(level1_csmar, f'select * from Fund_Allocation_m where EquityProportion > 0.3 and Symbol in {tuple(filtered_risky_symbol)}')
    Fund_Allocation_filter = pd.MultiIndex.from_frame(Fund_Allocation[['Date', 'Symbol']])

    return Fund_Allocation_filter


def generate_morningstar_rating(df):
    df = df.sort_values('tag', ascending=True)
    if df.shape[0] == 1:
        return df.MRAR_S.values[0], df.MRAR_RISK_S.values[0]
    elif df.shape[0] == 2:
        return df['MRAR_S'].values[0]*0.4 + df['MRAR_S'].values[1]*0.6, \
               df['MRAR_RISK_S'].values[0]*0.4 + df['MRAR_RISK_S'].values[1]*0.6
    else:
        return df['MRAR_S'].values[0]*0.2 + df['MRAR_S'].values[1]*0.3 + df['MRAR_S'].values[2]*0.5, \
               df['MRAR_RISK_S'].values[0]*0.2 + df['MRAR_RISK_S'].values[1]*0.3 + df['MRAR_RISK_S'].values[2]*0.5


def MRAR_generation(measure, rf):
    total_return = measure.dropna()
    temp_dates = measure.index
    risk_free = rf.loc[temp_dates]['rf']
    excess_return = (1 + total_return) / (1 + risk_free) - 1
    MRAR_0 = (1 + excess_return).prod() ** (12 / len(excess_return)) - 1
    MRAR_2 = (sum((1 + excess_return) ** (-2)) / len(excess_return)) ** (-6) - 1
    
    return MRAR_2, MRAR_0 - MRAR_2


def morning_star_generation(months_return, rf):
    three_year_data = months_return.loc[:, pd.notna(months_return).sum() >= 36].iloc[-36:].dropna(axis=1, how='any')
    five_year_data = months_return.loc[:, pd.notna(months_return).sum() >= 60].iloc[-60:].dropna(axis=1, how='any')
    ten_year_data = months_return.loc[:, pd.notna(months_return).sum() >= 120].iloc[-120:].dropna(axis=1, how='any')

    # compute MRAR
    three_year_result = three_year_data.apply(lambda x: MRAR_generation(x, rf), axis=0).apply(pd.Series).T.rename(columns={0: 'MRAR', 1: 'MRAR_RISK'}).droplevel(0)
    five_year_result = five_year_data.apply(lambda x: MRAR_generation(x, rf), axis=0).apply(pd.Series).T.rename(columns={0: 'MRAR', 1: 'MRAR_RISK'}).droplevel(0)
    ten_year_result = ten_year_data.apply(lambda x: MRAR_generation(x, rf), axis=0).apply(pd.Series).T.rename(columns={0: 'MRAR', 1: 'MRAR_RISK'}).droplevel(0)
    
    # generate score
    three_year_result['MRAR_S'] = pd.qcut(three_year_result['MRAR'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[1, 2, 3, 4, 5]).astype(int)
    three_year_result['MRAR_RISK_S'] = pd.qcut(three_year_result['MRAR_RISK'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[5, 4, 3, 2, 1]).astype(int)
    five_year_result['MRAR_S'] = pd.qcut(five_year_result['MRAR'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[1, 2, 3, 4, 5]).astype(int)
    five_year_result['MRAR_RISK_S'] = pd.qcut(five_year_result['MRAR_RISK'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[5, 4, 3, 2, 1]).astype(int)
    ten_year_result['MRAR_S'] = pd.qcut(ten_year_result['MRAR'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[1, 2, 3, 4, 5]).astype(int)
    ten_year_result['MRAR_RISK_S'] = pd.qcut(ten_year_result['MRAR_RISK'], q=[0, 0.1, 0.325, 0.675, 0.9, 1], labels=[5, 4, 3, 2, 1]).astype(int)

    # generate morningstar rating
    three_year_result['tag'] = 3
    five_year_result['tag'] = 5
    ten_year_result['tag'] = 10
    overall_data = pd.concat([three_year_result, five_year_result, ten_year_result])
    overall_rating = overall_data.groupby(level=0).apply(lambda x: generate_morningstar_rating(x)).apply(pd.Series).rename(columns={0: 'MRAR_Rating', 1: 'MRAR_RISK_Rating'})

    return overall_rating


def handle(Fund_Return_m, CH3, Classification):

    filtered_risky_symbol = get_filtered_risky_symbol(Classification)
    Fund_Allocation_filter = get_Fund_Allocation_filter(filtered_risky_symbol)

    m_return_of_risky_fund = Fund_Return_m.copy()

    filtered_m_return_of_risky_fund = m_return_of_risky_fund.loc[np.intersect1d(m_return_of_risky_fund.index, Fund_Allocation_filter)]

    regression_m_return_of_risky_fund = filtered_m_return_of_risky_fund.unstack()
    regression_m_return_of_risky_fund = regression_m_return_of_risky_fund.loc[:, pd.notna(regression_m_return_of_risky_fund).sum() > 12]

    morning_star = regression_m_return_of_risky_fund.rolling(window = 120, min_periods = 36).p_apply(lambda months_return: morning_star_generation(months_return, CH3[['rf']]))
    morning_star.index = morning_star.index + pd.offsets.MonthBegin(0)
    morning_star = morning_star.stack()
    m_return_of_risky_fund_index = m_return_of_risky_fund.index.set_levels(m_return_of_risky_fund.index.levels[0] + pd.offsets.MonthBegin(0), level=0)
    co_time = np.intersect1d(m_return_of_risky_fund_index, morning_star.index)
    morning_star = morning_star.loc[co_time]

    morning_star.name = 'morning_star'

    return morning_star


def upload(morning_star):
    morning_star.to_sql('morning_star', con=level3_factors, if_exists='replace')


@timeit('level3/morning_star')
def morning_star():
    Fund_Return_m, CH3, Classification = read_sql()
    morning_star = handle(Fund_Return_m, CH3, Classification)
    upload(morning_star)
morning_star()