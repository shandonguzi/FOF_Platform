

from functools import reduce

import numpy as np
import pandas as pd

from settings.database import level2_csmar, level0_joinquant, level1_csmar
from utils.mysql.get_sql import get_sql


def simple_truncate(factor_matrix, simple_truncate_proportion):
    simple_truncated = factor_matrix.apply(lambda x: x.dropna()[np.logical_and(x.dropna() > x.dropna().quantile(simple_truncate_proportion), x.dropna() < x.dropna().quantile(1 - simple_truncate_proportion))], axis=1)
    simple_truncated = simple_truncated.stack().index
    return simple_truncated

def ivol_truncate(return_matrix, ivol_truncate_proportion):
    ivol_condition = return_matrix.rolling(12).std()
    ivol_condition = ivol_condition.apply(lambda x: x.dropna()[np.logical_and(x.dropna() > x.dropna().quantile(ivol_truncate_proportion), x.dropna() < x.dropna().quantile(1 - ivol_truncate_proportion))], axis=1)
    ivol_condition.index = ivol_condition.index + pd.offsets.MonthBegin(0)
    ivol_condition = ivol_condition.stack().index
    return ivol_condition

def mkt_value_truncate(factor_matrix, value_threshold):
    temp_factor_matrix = factor_matrix.copy()
    temp_factor_matrix.index = factor_matrix.index - pd.offsets.MonthEnd(1)
    # EstimatedValue = get_sql(level2_csmar, f'select * from Fund_EstimatedValue_m where (Date, Symbol) in {tuple(temp_factor_matrix.stack().index)} and EstimatedValue_m > {value_threshold}')
    EstimatedValue = get_sql(level2_csmar, 'select * from Fund_EstimatedValue_m')
    EstimatedValue = EstimatedValue.set_index(['Date', 'Symbol'])
    EstimatedValue = EstimatedValue[(EstimatedValue.index.isin(tuple(temp_factor_matrix.stack().index))) & (EstimatedValue.EstimatedValue_m > value_threshold)]
    EstimatedValue = EstimatedValue.reset_index()
    EstimatedValue['Date'] = EstimatedValue.Date + pd.offsets.MonthBegin(0)
    mkt_value_condition = pd.MultiIndex.from_frame(EstimatedValue[['Date', 'Symbol']]) 
    return mkt_value_condition

def return_truncate(return_matrix, return_truncate_proportion):
    return_condition = return_matrix.apply(lambda x: x.dropna()[np.logical_and(x.dropna() > x.dropna().quantile(return_truncate_proportion), x.dropna() < x.dropna().quantile(1 - return_truncate_proportion))], axis=1)
    return_condition.index = return_condition.index + pd.offsets.MonthBegin(0)
    return_condition = return_condition.stack().index
    return return_condition

def drop_A_or_other(one_time, funds_info):
    if one_time.name == pd.to_datetime('2022-11-01'):
        pass
    symbols = one_time[pd.notna(one_time)].index
    temp_funds_info = funds_info[funds_info.Symbol.isin(symbols)]
    duplicates = temp_funds_info.name.str.strip('D').str.strip('C').str.strip('B').str.strip('A').duplicated(keep=False)
    not_end_with_C = temp_funds_info.name.apply(lambda x: 'C' != x[-1])
    to_be_droped = np.logical_and(duplicates, not_end_with_C)
    temp_funds_info = temp_funds_info[~ to_be_droped]
    one_time[~ one_time.index.isin(temp_funds_info.Symbol)] = np.nan

    return one_time


def AC_type_truncate(factor_matrix):
    funds_info = get_sql(level0_joinquant, f'select * from Fund_MainInfo where Symbol in {tuple(factor_matrix.columns)}')
    AC_type_condition = factor_matrix.copy().apply(lambda one_time: drop_A_or_other(one_time, funds_info), axis=1).stack().index
    return AC_type_condition


def not_tradable_truncate(factor_matrix):
    # Fund_PurchRedChg = get_sql(level1_csmar, f'select * from Fund_PurchRedChg where (Date, Symbol) in {tuple(factor_matrix.stack().index)}')
    Fund_PurchRedChg = get_sql(level1_csmar, 'select * from Fund_PurchRedChg')
    Fund_PurchRedChg = Fund_PurchRedChg.set_index(['Date', 'Symbol'])
    Fund_PurchRedChg = Fund_PurchRedChg[Fund_PurchRedChg.index.isin(tuple(factor_matrix.stack().index))]
    Fund_PurchRedChg = Fund_PurchRedChg.reset_index()
    not_tradable = pd.MultiIndex.from_frame(Fund_PurchRedChg[['Date', 'Symbol']]) 
    return not_tradable


def truncate_factor(factor_matrix, return_matrix, simple_truncate_proportion, ivol_truncate_proportion, value_threshold, return_truncate_proportion):

    simple_truncate_condition = simple_truncate(factor_matrix, simple_truncate_proportion)
    ivol_truncate_condition = ivol_truncate(return_matrix, ivol_truncate_proportion)
    mkt_value_truncate_condition = mkt_value_truncate(factor_matrix, value_threshold)
    return_truncate_condition = return_truncate(return_matrix, return_truncate_proportion)
    AC_type_condition = AC_type_truncate(factor_matrix)
    not_tradable_truncate_condition = not_tradable_truncate(factor_matrix)

    intersect_condition = reduce(np.intersect1d, [factor_matrix.stack().index, simple_truncate_condition, ivol_truncate_condition, mkt_value_truncate_condition, return_truncate_condition, AC_type_condition])
    exclude_condition = np.intersect1d(intersect_condition, not_tradable_truncate_condition.values, return_indices=True)[1]
    final_index = np.delete(intersect_condition, exclude_condition)

    return final_index


def simple_truncate_factor(factor_matrix):
    
    AC_type_condition = AC_type_truncate(factor_matrix)
    not_tradable_truncate_condition = not_tradable_truncate(factor_matrix)

    intersect_condition = np.intersect1d(factor_matrix.stack().index, AC_type_condition)
    exclude_condition = np.intersect1d(intersect_condition, not_tradable_truncate_condition.values, return_indices=True)[1]
    final_index = np.delete(intersect_condition, exclude_condition)

    return final_index


def middle_truncate_factor(factor_matrix):
    
    AC_type_condition = AC_type_truncate(factor_matrix)
    mkt_value_truncate_condition = mkt_value_truncate(factor_matrix, 1e7)
    not_tradable_truncate_condition = not_tradable_truncate(factor_matrix)

    intersect_condition = reduce(np.intersect1d, [factor_matrix.stack().index, AC_type_condition, mkt_value_truncate_condition])
    exclude_condition = np.intersect1d(intersect_condition, not_tradable_truncate_condition.values, return_indices=True)[1]
    final_index = np.delete(intersect_condition, exclude_condition)

    return final_index

