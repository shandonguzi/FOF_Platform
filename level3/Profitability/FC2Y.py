
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

    data_is = data_is[data_is.ReportType == 'A'][['Stkcd', 'Date', 'Revenue', 'SellExp', 'AdminExp', 'RnDExp']]
    data_is['FC2Y'] = (data_is['SellExp'] + data_is['AdminExp'] + data_is['RnDExp']) / data_is['Revenue']
    
    fc2y = data_is[['Stkcd', 'Date', 'FC2Y']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = fc2y.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    fc2y = pd.concat([fc2y, to_today], axis=0)
    fc2y = pd.merge(fc2y, Stock_last_day, on=['Stkcd'])
    fc2y = fc2y.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    fc2y = fc2y[(fc2y.Date <= fc2y.LastDay)]
    fc2y = fc2y.drop(['LastDay'], axis=1)
    fc2y['Date'] = fc2y['Date'] + pd.offsets.MonthBegin(0)

    return fc2y


def upload(fc2y):
    fc2y.to_sql('FC2Y', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/FC2Y')
def FC2Y():
    data_is, Stock_last_day = read_sql()
    fc2y = handle(data_is, Stock_last_day)
    upload(fc2y)