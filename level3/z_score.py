from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    capm_α = get_sql(level3_factors, 'capm_α', index='Date', columns='Symbol', values='capm_α')
    capm_α_ε = get_sql(level3_factors, 'capm_α_ε', index='Date', columns='Symbol', values='capm_α_ε')
    svc_α = get_sql(level3_factors, 'svc_α', index='Date', columns='Symbol', values='svc_α')
    svc_α_ε = get_sql(level3_factors, 'svc_α_ε', index='Date', columns='Symbol', values='svc_α_ε')
    svc_α_truncated = get_sql(level3_factors, 'svc_α_truncated', index='Date', columns='Symbol', values='svc_α_truncated')
    active_share = get_sql(level3_factors, 'active_share', index='Date', columns='Symbol', values='active_share')
    connected_companies_portfolio_capm = get_sql(level3_factors, 'connected_companies_portfolio_capm', index='Date', columns='Symbol', values='connected_companies_portfolio_capm')
    connected_companies_portfolio_svc = get_sql(level3_factors, 'connected_companies_portfolio_svc', index='Date', columns='Symbol', values='connected_companies_portfolio_svc')
    industry_concentration = get_sql(level3_factors, 'industry_concentration', index='Date', columns='Symbol', values='industry_concentration')
    return_gap = get_sql(level3_factors, 'return_gap', index='Date', columns='Symbol', values='return_gap')
    ME = get_sql(level3_factors, 'ME', index='Date', columns='Symbol', values='EstimatedValue_m')
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index='Date', columns='Symbol', values='Fund_Return_m')

    return capm_α, capm_α_ε, svc_α, svc_α_ε, svc_α_truncated, active_share, connected_companies_portfolio_capm, connected_companies_portfolio_svc, industry_concentration, return_gap, ME, Fund_Return_m

def set_index_to_month_begin(df):
    if df.index.days_in_month[0] != 1:
        df.index = df.index + pd.DateOffset(days=1)
    return df

def get_z_score(α_matrix):
    '''
    get z score of alpha matrix
    :param α_matrix: pd.DataFrame, index is datetime, columns are fund codes
    :return: pd.DataFrame, index is datetime, columns are fund codes
    '''
    α_matrix = set_index_to_month_begin(α_matrix)
    return (
        α_matrix.subtract(α_matrix.mean(axis=1), axis=0)
        .div(α_matrix.std(axis=1), axis=0)
        .resample('M')
        .last()
        .ffill()
        .fillna(0)
    )

def handle(capm_α, capm_α_ε, svc_α, svc_α_ε, svc_α_truncated, active_share, connected_companies_portfolio_capm, connected_companies_portfolio_svc, industry_concentration, return_gap, ME, Fund_Return_m):

    z_capm_α = get_z_score(capm_α)
    z_capm_α_ε = get_z_score(capm_α_ε)
    z_svc_α = get_z_score(svc_α)
    z_svc_α_ε = get_z_score(svc_α_ε)
    z_active_share = get_z_score(active_share)
    z_connected_companies_portfolio_capm = get_z_score(connected_companies_portfolio_capm)
    z_connected_companies_portfolio_svc = get_z_score(connected_companies_portfolio_svc)
    z_industry_concentration = get_z_score(industry_concentration)
    z_return_gap = get_z_score(return_gap)
    z_ME = get_z_score(ME)


    co_funds = reduce(np.intersect1d, (z_capm_α.columns, z_capm_α_ε.columns, z_svc_α.columns,  z_svc_α_ε.columns, z_active_share.columns, z_connected_companies_portfolio_capm.columns, \
                                    z_connected_companies_portfolio_svc.columns, z_industry_concentration.columns, z_return_gap.columns, z_ME.columns))

    co_time = reduce(np.intersect1d, (z_capm_α.index, z_capm_α_ε.index, z_svc_α.index,  z_svc_α_ε.index, z_active_share.index, z_connected_companies_portfolio_capm.index, \
                                    z_connected_companies_portfolio_svc.index, z_industry_concentration.index, z_return_gap.index, z_ME.index))
    z_score = z_capm_α.loc[co_time, co_funds] + z_capm_α_ε.loc[co_time, co_funds] + z_svc_α.loc[co_time, co_funds] + z_svc_α_ε.loc[co_time, co_funds] + z_active_share.loc[co_time, co_funds] + z_connected_companies_portfolio_capm.loc[co_time, co_funds] + \
                z_connected_companies_portfolio_svc.loc[co_time, co_funds] + z_industry_concentration.loc[co_time, co_funds] + z_return_gap.loc[co_time, co_funds] + z_ME.loc[co_time, co_funds]

    z_score.index = z_score.index - pd.offsets.MonthBegin(1)
    z_score = z_score.stack()
    z_score.name = 'z_score'

    return z_score

def upload(z_score):
    z_score.to_sql('z_score', con=level3_factors, if_exists='replace')

@timeit('level3/z_score')
def z_score():
    capm_α, capm_α_ε, svc_α, svc_α_ε, svc_α_truncated, active_share, connected_companies_portfolio_capm, connected_companies_portfolio_svc, industry_concentration, return_gap, ME, Fund_Return_m = read_sql()
    z_score = handle(capm_α, capm_α_ε, svc_α, svc_α_ε, svc_α_truncated, active_share, connected_companies_portfolio_capm, connected_companies_portfolio_svc, industry_concentration, return_gap, ME, Fund_Return_m)
    upload(z_score)
