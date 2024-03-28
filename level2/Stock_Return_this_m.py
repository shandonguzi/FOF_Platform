
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():

    Stock_Return_d = get_sql(level1_csmar, 'TRD_Dalyr')
    
    return Stock_Return_d


def handle(Stock_Return_d):

    Stock_Return_d.loc[:, 'TotalShares'] = Stock_Return_d['MKTValue'] / Stock_Return_d['Close']
    Stock_Return_d = Stock_Return_d[['Stkcd', 'Date', 'Close', 'TradingVolume', 'MKTValue', 'TotalShares']]
    
    if Stock_Return_d.Date.max().month == this_month_begin.month:

        Stock_Return_d = Stock_Return_d[Stock_Return_d.Date >= last_month_begin_s]

        lm_last_price = Stock_Return_d[Stock_Return_d.Date.dt.month == last_month_begin.month].groupby('Stkcd').agg({'Date': 'max'}).merge(Stock_Return_d, on=['Stkcd', 'Date'])[['Stkcd', 'Close']]
        cm_last_price = Stock_Return_d[Stock_Return_d.Date.dt.month == this_month_begin.month].groupby('Stkcd').agg({'Date': 'max'}).merge(Stock_Return_d, on=['Stkcd', 'Date'])[['Stkcd', 'Close']]
        
        cm_trading_volume = Stock_Return_d[Stock_Return_d.Date.dt.month == this_month_begin.month].groupby('Stkcd').agg({'TradingVolume': 'sum'}).reset_index()
        
        cm_total_shares = Stock_Return_d[Stock_Return_d.Date.dt.month == this_month_begin.month].groupby('Stkcd').agg({'TotalShares': 'mean'}).reset_index()
        
        cm_mkt_value = Stock_Return_d[Stock_Return_d.Date.dt.month == this_month_begin.month].groupby('Stkcd').apply(lambda x: x.sort_values('Date').iloc[-1]['MKTValue']).reset_index().rename(columns={0: 'MKTValue'})
    
    else:
        Stock_Return_d = Stock_Return_d[Stock_Return_d.Date >= last_2_month_begin_s]

        lm_last_price = Stock_Return_d[Stock_Return_d.Date.dt.month == last_2_month_begin.month].groupby('Stkcd').agg({'Date': 'max'}).merge(Stock_Return_d, on=['Stkcd', 'Date'])[['Stkcd', 'Close']]
        cm_last_price = Stock_Return_d[Stock_Return_d.Date.dt.month == last_month_begin.month].groupby('Stkcd').agg({'Date': 'max'}).merge(Stock_Return_d, on=['Stkcd', 'Date'])[['Stkcd', 'Close']]
        
        cm_trading_volume = Stock_Return_d[Stock_Return_d.Date.dt.month == last_month_begin.month].groupby('Stkcd').agg({'TradingVolume': 'sum'}).reset_index()
        
        cm_total_shares = Stock_Return_d[Stock_Return_d.Date.dt.month == last_month_begin.month].groupby('Stkcd').agg({'TotalShares': 'mean'}).reset_index()
        
        cm_mkt_value = Stock_Return_d[Stock_Return_d.Date.dt.month == last_month_begin.month].groupby('Stkcd').apply(lambda x: x.sort_values('Date').iloc[-1]['MKTValue']).reset_index().rename(columns={0: 'MKTValue'})

    cm_return = pd.merge(cm_last_price, lm_last_price, on='Stkcd', how='inner', suffixes=('_cm', '_lm'))
    cm_return = pd.merge(cm_return, cm_trading_volume, on='Stkcd', how='inner')
    cm_return = pd.merge(cm_return, cm_total_shares, on='Stkcd', how='inner')
    cm_return = pd.merge(cm_return, cm_mkt_value, on='Stkcd', how='inner')
    cm_return['RealPctChange'] = (cm_return['Close_cm'] - cm_return['Close_lm']) / cm_return['Close_lm']
    
    if Stock_Return_d.Date.max().month == this_month_begin.month:
        cm_return['Date'] = pd.to_datetime(this_month_begin)
    else:
        cm_return['Date'] = pd.to_datetime(last_month_begin)

    cm_return.drop(['Close_cm', 'Close_lm'], axis=1, inplace=True)
    cm_return = cm_return[['Stkcd', 'Date', 'RealPctChange', 'TradingVolume', 'TotalShares', 'MKTValue']]

    return cm_return


def upload(cm_return):
    cm_return.to_sql('cm_return', con=level2_csmar, if_exists='replace', index=False)


@timeit('level2/Stock_Return_this_m')
def Stock_Return_this_m():
    Stock_Return_d = read_sql()
    cm_return = handle(Stock_Return_d)
    upload(cm_return)

