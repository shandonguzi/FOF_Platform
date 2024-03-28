
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    data_is = get_sql(level1_csmar, 'FS_Comins')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_is, Stock_last_day


def handle(data_is, Stock_last_day):

    data_is = data_is[data_is.ReportType == 'A'][['Stkcd', 'Date', 'Revenue', 'COGS']]
    data_is['PCM'] = (data_is['Revenue'] - data_is['COGS']) / data_is['Revenue']
    
    pcm = data_is[['Stkcd', 'Date', 'PCM']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = pcm.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    pcm = pd.concat([pcm, to_today], axis=0)
    pcm = pd.merge(pcm, Stock_last_day, on=['Stkcd'])
    pcm = pcm.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    pcm = pcm[(pcm.Date <= pcm.LastDay)]
    pcm = pcm.drop(['LastDay'], axis=1)
    pcm['Date'] = pcm['Date'] + pd.offsets.MonthBegin(0)

    return pcm


def upload(pcm):
    pcm.to_sql('PCM', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/PCM')
def PCM():
    data_is, Stock_last_day = read_sql()
    pcm = handle(data_is, Stock_last_day)
    upload(pcm)