
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

    data_bs = data_bs[data_bs.ReportType == 'A'][['Stkcd', 'Date', 'MonetaryCapital', 'ShortTermInvestment', 'TotalAssets']]
    data_bs['C'] = (data_bs['MonetaryCapital'] + data_bs['ShortTermInvestment']) / data_bs['TotalAssets']
    
    c = data_bs.reset_index()[['Stkcd', 'Date', 'C']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = c.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    c = pd.concat([c, to_today], axis=0)
    c = c.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    c = pd.merge(c, Stock_last_day, on=['Stkcd'])
    c = c[(c.Date <= c.LastDay)]
    c = c.drop(['LastDay'], axis=1)
    c['Date'] = c['Date'] + pd.offsets.MonthBegin(0)

    return c


def upload(c):
    c.to_sql('C', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/C')
def C():
    data_bs, Stock_last_day = read_sql()
    c = handle(data_bs, Stock_last_day)
    upload(c)