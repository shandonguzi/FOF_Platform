
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
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'Revenue', 'MonetaryCapital', 'TradingFinAssets', 'ShortTermBorrow', 'LongTermBorrow', 'TotalShrhldrEquity']]
    finan_rept['ATO'] = finan_rept['Revenue'] / (finan_rept['ShortTermBorrow'] + finan_rept['LongTermBorrow'] + finan_rept['TotalShrhldrEquity'] - finan_rept['MonetaryCapital'] - finan_rept['TradingFinAssets'])
    
    ato = finan_rept[['Stkcd', 'Date', 'ATO']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = ato.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    ato = pd.concat([ato, to_today], axis=0)
    ato = ato.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    ato = pd.merge(ato, Stock_last_day, on=['Stkcd'])
    ato = ato[(ato.Date <= ato.LastDay)]
    ato = ato.drop(['LastDay'], axis=1)
    ato['Date'] = ato['Date'] + pd.offsets.MonthBegin(0)

    return ato


def upload(ato):
    ato.to_sql('ATO', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ATO')
def ATO():
    data_is, data_bs, Stock_last_day = read_sql()
    ato = handle(data_is, data_bs, Stock_last_day)
    upload(ato)