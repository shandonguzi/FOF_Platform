
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return TRD_Dalyr, Stock_last_day


def handle(TRD_Dalyr, Stock_last_day):

    TRD_Dalyr = TRD_Dalyr[['Stkcd', 'Date', 'Close', 'CurrencyMKTValue']]
    TRD_Dalyr = TRD_Dalyr.set_index(['Date']).groupby(['Stkcd']).resample('Y').last().drop(['Stkcd'], axis=1).ffill().dropna()
    TRD_Dalyr['CurrentShare'] = TRD_Dalyr['CurrencyMKTValue'] / TRD_Dalyr['Close']
    TRD_Dalyr['LastYearCurrentShare'] = TRD_Dalyr.groupby(['Stkcd'])['CurrentShare'].shift(1)
    TRD_Dalyr['NSI'] = np.log(TRD_Dalyr['CurrentShare'] / TRD_Dalyr['LastYearCurrentShare'])
    
    nsi = TRD_Dalyr.reset_index()[['Stkcd', 'Date', 'NSI']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = nsi.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    nsi = pd.concat([nsi, to_today], axis=0)
    nsi = pd.merge(nsi, Stock_last_day, on=['Stkcd'])
    nsi = nsi.set_index('Date').groupby(['Stkcd']).resample('M').last().ffill().drop(['Stkcd'], axis=1).reset_index()
    nsi = nsi[(nsi.Date <= nsi.LastDay)]
    nsi = nsi.drop(['LastDay'], axis=1)
    nsi['Date'] = nsi['Date'] + pd.offsets.MonthBegin(0)

    return nsi


def upload(nsi):
    nsi.to_sql('NSI', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/NSI')
def NSI():
    TRD_Dalyr, Stock_last_day = read_sql()
    nsi = handle(TRD_Dalyr, Stock_last_day)
    upload(nsi)