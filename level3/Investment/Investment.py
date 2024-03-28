
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

    data_bs = data_bs[(data_bs.ReportType == 'A') & (data_bs.Date.dt.month == 12)][['Stkcd', 'Date', 'TotalAssets']]
    data_bs = data_bs.set_index(['Date']).groupby(['Stkcd']).resample('Y').ffill().dropna().drop(['Stkcd'], axis=1)
    data_bs['LastYearTotalAssets'] = data_bs.groupby(['Stkcd'])['TotalAssets'].shift(1)
    data_bs['Investment'] = (data_bs['TotalAssets'] / data_bs['LastYearTotalAssets'] - 1)
    
    investment = data_bs.reset_index().drop(['TotalAssets', 'LastYearTotalAssets'], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    to_today = investment.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    investment = pd.concat([investment, to_today], axis=0)
    investment = pd.merge(investment, Stock_last_day, on=['Stkcd'])
    investment = investment.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    investment = investment[(investment.Date <= investment.LastDay)]
    investment = investment.drop(['LastDay'], axis=1)
    investment['Date'] = investment['Date'] + pd.offsets.MonthBegin(0)

    return investment


def upload(investment):
    investment.to_sql('Investment', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/Investment')
def Investment():
    data_bs, Stock_last_day = read_sql()
    investment = handle(data_bs, Stock_last_day)
    upload(investment)