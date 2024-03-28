
import numpy as np

from settings.database import level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    Fund_EstimatedValue_m = get_sql(level2_csmar, 'Fund_EstimatedValue_m', index_cols=['Date', 'Symbol'])
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m', index_cols=['Date', 'Symbol'])

    return Fund_EstimatedValue_m, Fund_Return_m


def handle(Fund_EstimatedValue_m, Fund_Return_m):
    co_index = np.intersect1d(Fund_EstimatedValue_m.index, Fund_Return_m.index)
    ME = Fund_EstimatedValue_m.loc[co_index]
    return ME


def upload(ME):
    ME.to_sql('ME', con=level3_factors, if_exists='replace')


@timeit('level3/ME')
def ME():
    Fund_EstimatedValue_m, Fund_Return_m = read_sql()
    ME = handle(Fund_EstimatedValue_m, Fund_Return_m)
    upload(ME)