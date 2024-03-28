
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, Stock_last_day


def handle(data_bs, Stock_last_day):

    data_bs = data_bs[data_bs.ReportType == 'A'][['Stkcd', 'Date', 'TotalAssets']]
    data_bs['AT'] = data_bs['TotalAssets']
    
    at = data_bs[['Stkcd', 'Date', 'AT']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = at.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    at = pd.concat([at, to_today], axis=0)
    at = pd.merge(at, Stock_last_day, on=['Stkcd'])
    at = at.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    at = at[(at.Date <= at.LastDay)]
    at = at.drop(['LastDay'], axis=1)
    at['Date'] = at['Date'] + pd.offsets.MonthBegin(0)

    return at


def upload(at):
    at.to_sql('AT', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/AT')
def AT():
    data_bs, Stock_last_day = read_sql()
    at = handle(data_bs, Stock_last_day)
    upload(at)