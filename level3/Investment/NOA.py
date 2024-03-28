
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

    # 将报表准则更新后的新数据覆盖至12-31
    # data_bs = data_bs[(data_bs.ReportType == 'A') & ((data_bs.Date.dt.month == 12) | (data_bs.Date.dt.month == 1))][['Stkcd', 'Date', 'TotalAssets', 'MonetaryCapital', 'TradingFinAssets', 'ShortTermBorrow', 'LongTermBorrow', 'TotalShrhldrEquity']]
    # data_bs_month_1 = data_bs[data_bs.Date.dt.month == 1]
    # data_bs_month_1['Date'] = data_bs_month_1['Date'] - pd.offsets.YearEnd(1)
    # data_bs = pd.concat([data_bs[data_bs.Date.dt.month == 12], data_bs_month_1], axis=0).drop_duplicates(keep='last')
    data_bs = data_bs[(data_bs.ReportType == 'A') & (data_bs.Date.dt.month == 12)][['Stkcd', 'Date', 'TotalAssets', 'MonetaryCapital', 'TradingFinAssets', 'ShortTermBorrow', 'LongTermBorrow', 'TotalShrhldrEquity']]
    data_bs = data_bs.set_index(['Date']).groupby(['Stkcd']).resample('Y').ffill().dropna().drop(['Stkcd'], axis=1)
    data_bs['OperatingAssets'] = data_bs['TotalAssets'] - data_bs['MonetaryCapital'] - data_bs['TradingFinAssets']
    data_bs['OperatingLiabilities'] = data_bs['TotalAssets'] - data_bs['ShortTermBorrow'] - data_bs['LongTermBorrow'] - data_bs['TotalShrhldrEquity']
    data_bs['LastYearTotalAssets'] = data_bs.groupby(['Stkcd'])['TotalAssets'].shift(1)
    data_bs['NOA'] = (data_bs['OperatingAssets'] - data_bs['OperatingLiabilities']) / data_bs['LastYearTotalAssets']
    
    noa = data_bs.reset_index()[['Stkcd', 'Date', 'NOA']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = noa.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    noa = pd.concat([noa, to_today], axis=0)
    noa = pd.merge(noa, Stock_last_day, on=['Stkcd'])
    noa = noa.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    noa = noa[(noa.Date <= noa.LastDay)]
    noa = noa.drop(['LastDay'], axis=1)
    noa['Date'] = noa['Date'] + pd.offsets.MonthBegin(0)

    return noa


def upload(noa):
    noa.to_sql('NOA', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/NOA')
def NOA():
    data_bs, Stock_last_day = read_sql()
    noa = handle(data_bs, Stock_last_day)
    upload(noa)