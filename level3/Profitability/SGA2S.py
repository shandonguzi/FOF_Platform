
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

    data_is = data_is[data_is.ReportType == 'A'][['Stkcd', 'Date', 'Revenue', 'SellExp', 'AdminExp']]
    data_is['SGA2S'] = (data_is['SellExp'] + data_is['AdminExp']) / data_is['Revenue']
    
    sga2s = data_is[['Stkcd', 'Date', 'SGA2S']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = sga2s.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    sga2s = pd.concat([sga2s, to_today], axis=0)
    sga2s = pd.merge(sga2s, Stock_last_day, on=['Stkcd'])
    sga2s = sga2s.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    sga2s = sga2s[(sga2s.Date <= sga2s.LastDay)]
    sga2s = sga2s.drop(['LastDay'], axis=1)
    sga2s['Date'] = sga2s['Date'] + pd.offsets.MonthBegin(0)

    return sga2s


def upload(sga2s):
    sga2s.to_sql('SGA2S', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/SGA2S')
def SGA2S():
    data_is, Stock_last_day = read_sql()
    sga2s = handle(data_is, Stock_last_day)
    upload(sga2s)