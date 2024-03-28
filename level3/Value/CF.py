
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_cfd = get_sql(level1_csmar, 'FS_Comscfd')
    data_bs = get_sql(level1_csmar, 'FS_Combas')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_cfd, data_bs, Stock_last_day


def handle(data_cfd, data_bs, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_cfd, data_bs])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'NetCFO', 'BuyPPE', 'TotalShrhldrEquity', 'MinorityInterest']]
    finan_rept['CF'] = (finan_rept['NetCFO'] - finan_rept['BuyPPE']) / (finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest'])
    
    cf = finan_rept[['Stkcd', 'Date', 'CF']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = cf.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    cf = pd.concat([cf, to_today], axis=0)
    cf = cf.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    cf = pd.merge(cf, Stock_last_day, on=['Stkcd'])
    cf = cf[(cf.Date <= cf.LastDay)]
    cf = cf.drop(['LastDay'], axis=1)
    cf['Date'] = cf['Date'] + pd.offsets.MonthBegin(0)

    return cf


def upload(cf):
    cf.to_sql('CF', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CF')
def CF():
    data_cfd, data_bs, Stock_last_day = read_sql()
    cf = handle(data_cfd, data_bs, Stock_last_day)
    upload(cf)