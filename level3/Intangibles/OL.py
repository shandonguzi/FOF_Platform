
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    data_is = get_sql(level1_csmar, 'FS_Comins')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, data_is, Stock_last_day


def handle(data_bs, data_is, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_bs, data_is])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'TotalAssets', 'COGS', 'SellExp', 'AdminExp']]
    finan_rept = finan_rept.set_index(['Date']).groupby(['Stkcd']).resample('Y').ffill().dropna().drop(['Stkcd'], axis=1)
    finan_rept['OL'] = (finan_rept['COGS'] + finan_rept['SellExp'] + finan_rept['AdminExp']) / finan_rept['TotalAssets']

    ol = finan_rept.reset_index()[['Stkcd', 'Date', 'OL']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = ol.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    ol = pd.concat([ol, to_today], axis=0)
    ol = ol.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    ol = pd.merge(ol, Stock_last_day, on=['Stkcd'])
    ol = ol[(ol.Date <= ol.LastDay)]
    ol = ol.drop(['LastDay'], axis=1)
    ol['Date'] = ol['Date'] + pd.offsets.MonthBegin(0)

    return ol


def upload(ol):
    ol.to_sql('OL', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/OL')
def OL():
    data_bs, data_is, Stock_last_day = read_sql()
    ol = handle(data_bs, data_is, Stock_last_day)
    upload(ol)