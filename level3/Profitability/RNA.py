
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_is = get_sql(level1_csmar, 'FS_Comins')
    data_bs = get_sql(level1_csmar, 'FS_Combas')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_is, data_bs, Stock_last_day


def handle(data_is, data_bs, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_is, data_bs])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'OperatingProfit', 'MonetaryCapital', 'TradingFinAssets', 'ShortTermBorrow', 'LongTermBorrow', 'TotalShrhldrEquity']]
    finan_rept['RNA'] = finan_rept['OperatingProfit'] / (finan_rept['ShortTermBorrow'] + finan_rept['LongTermBorrow'] + finan_rept['TotalShrhldrEquity'] - finan_rept['MonetaryCapital'] - finan_rept['TradingFinAssets'])
    
    rna = finan_rept[['Stkcd', 'Date', 'RNA']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = rna.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    rna = pd.concat([rna, to_today], axis=0)
    rna = rna.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    rna = pd.merge(rna, Stock_last_day, on=['Stkcd'])
    rna = rna[(rna.Date <= rna.LastDay)]
    rna = rna.drop(['LastDay'], axis=1)
    rna['Date'] = rna['Date'] + pd.offsets.MonthBegin(0)

    return rna


def upload(rna):
    rna.to_sql('RNA', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/RNA')
def RNA():
    data_is, data_bs, Stock_last_day = read_sql()
    rna = handle(data_is, data_bs, Stock_last_day)
    upload(rna)