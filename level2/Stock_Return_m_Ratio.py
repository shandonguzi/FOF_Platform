
import time

from functools import reduce
from pandas import Timestamp, offsets
import numpy as np
import pandas as pd
from settings.database import level1_csmar, level2_csmar

import utils.mysql.p_to_sql
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    TRD_Dalyr = get_sql(level1_csmar, 'TRD_Dalyr')

    try:
        df_past = get_sql(level2_csmar, 'Stock_Return_m_Ratio')
        return df_past, TRD_Dalyr
    except Exception:
        return np.nan, TRD_Dalyr

def handle(df_past, TRD_Dalyr):
    # Reversal
    Reversal = TRD_Dalyr.sort_values(['Date']).set_index('Date')[['Stkcd', 'RealPctChange']]
    Reversal['RealPctChange'] = np.log(Reversal['RealPctChange']+1)
    Reversal = (np.exp(Reversal.groupby(['Stkcd']).rolling(20).sum())-1).reset_index()
    Reversal.rename(columns={'RealPctChange': 'Reversal'}, inplace=True)

    # Volatility
    Volatility = TRD_Dalyr.sort_values(['Date']).set_index('Date')[['Stkcd', 'RealPctChange']]
    Volatility1M = Volatility.groupby(['Stkcd']).rolling(20).std().reset_index().rename(columns={'RealPctChange': 'Volatility1M'})
    VolatilityMAX = Volatility.groupby(['Stkcd']).rolling(20).max().reset_index().rename(columns={'RealPctChange': 'VolatilityMAX'})
    Volatility = pd.merge(Volatility1M, VolatilityMAX, on=['Stkcd', 'Date'])

    # Turnover
    TRD_Dalyr['Turnover'] = TRD_Dalyr['TradingVolume'] * TRD_Dalyr['Close']  / TRD_Dalyr['MKTValue']
    Turnover = TRD_Dalyr.sort_values(['Date']).set_index('Date')[['Stkcd', 'Turnover']]
    Turnover12M = Turnover.groupby(['Stkcd']).rolling(250).mean().reset_index().rename(columns={'Turnover': 'Turnover12M'})
    TurnoverAbn = Turnover.groupby(['Stkcd']).rolling(20).mean().reset_index().rename(columns={'Turnover': 'TurnoverAbn'})
    TurnoverAbn['TurnoverAbn'] = TurnoverAbn['TurnoverAbn'] / Turnover12M['Turnover12M']
    Turnover = pd.merge(Turnover12M, TurnoverAbn, on=['Stkcd', 'Date'])

    # Illiquidity
    TRD_Dalyr['Illiquidity'] = abs(TRD_Dalyr['RealPctChange']) / TRD_Dalyr['TradingVolume']

    # 整合数据，降频到月度，每月末删除上个月末市值最低30%的股票作为当月股票池
    TRD_Dalyr = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date'], how='inner'), [TRD_Dalyr, Reversal, Volatility, Turnover])
    TRD_Dalyr = TRD_Dalyr.set_index('Date').groupby('Stkcd').resample('M').last().drop(['Stkcd'], axis=1)
    TRD_Dalyr['LastMonthMktv'] = TRD_Dalyr.groupby(['Stkcd'])['MKTValue'].shift(1)
    TRD_Dalyr.dropna(inplace=True)
    TRD_Dalyr = TRD_Dalyr.groupby(level=1).apply(lambda x: x[x['LastMonthMktv'] >= x['LastMonthMktv'].quantile(0.3)]).droplevel(0)

    # 整合财报数据
    data_bs = pd.read_sql('select * from FS_Combas where ReportType = "A";', level1_csmar)
    data_ins = pd.read_sql('select * from FS_Comins where ReportType = "A";', level1_csmar)
    data_cfi = pd.read_sql('select * from FS_Comscfi where ReportType = "A";', level1_csmar)
    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date'], how='outer'), [data_bs, data_ins, data_cfi])
    finan_rept = finan_rept[finan_rept['Date'].dt.month != 1].reset_index(drop=True)

    # 单ffill只会填充resample前不存在的行，如果此行存在且有空值，一次ffill不会填充，必须再来一次
    finan_rept = finan_rept.set_index(['Date']).groupby(['Stkcd']).resample('Q').fillna(method='ffill').fillna(method='ffill').dropna()

    # Last year data
    finan_rept_this_year = finan_rept[['TotalAssets', 'TotalCurAssets', 'CashEnd', 'CashEquiEnd', 'TotalCurLiab', 
                                    'NonCurLiabIn1Y', 'NotesPayable', 'TaxesPayable', 'DepreciationFaOgaBba', 'AmortizationIntang']].copy()
    finan_rept_this_year.loc[:, 'STD'] = finan_rept_this_year['NonCurLiabIn1Y'] + finan_rept_this_year['NotesPayable']
    finan_rept_this_year.loc[:, 'DP'] = finan_rept_this_year['DepreciationFaOgaBba'] + finan_rept_this_year['AmortizationIntang']
    finan_rept_this_year.loc[:, 'CASH'] = finan_rept_this_year['CashEnd'] + finan_rept_this_year['CashEquiEnd']
    finan_rept_last_year = finan_rept_this_year.groupby(['Stkcd']).shift(4)
    
    # Investment
    Investment = pd.DataFrame((finan_rept_this_year['TotalAssets'] / finan_rept_last_year['TotalAssets'] - 1)).rename(columns={'TotalAssets': 'Investment'})

    # Accrual
    Accrual = (finan_rept_this_year['TotalCurAssets'] - finan_rept_last_year['TotalCurAssets']) - \
            (finan_rept_this_year['CASH'] - finan_rept_last_year['CASH']) - \
            (finan_rept_this_year['TotalCurLiab'] - finan_rept_last_year['TotalCurLiab']) + \
            (finan_rept_this_year['STD'] - finan_rept_last_year['STD']) - \
            (finan_rept_this_year['TaxesPayable'] - finan_rept_last_year['TaxesPayable']) - \
            finan_rept_this_year['DP']
    AccrualComponent = pd.DataFrame((2*Accrual/(finan_rept_this_year['TotalAssets'] + finan_rept_last_year['TotalAssets'])), columns=['AccrualComponent'])

    OperatingAssets = finan_rept['TotalAssets'] - finan_rept['MonetaryCapital'] - finan_rept['TradingFinAssets']
    OperatingLiabilities = finan_rept['TotalAssets'] - finan_rept['ShortTermBorrow'] - finan_rept['LongTermBorrow'] - finan_rept['TotalShrhldrEquity']
    NOA = pd.DataFrame(((OperatingAssets - OperatingLiabilities) / finan_rept_last_year['TotalAssets']), columns=['NOA'])

    # other ratios
    finan_rept['ROE'] = (finan_rept['NetProfit'] - finan_rept['MinorityInterest_y']) / (finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest_x'])
    finan_rept['Investment'] = Investment
    finan_rept['AccrualComponent'] = AccrualComponent
    finan_rept['NOA'] = NOA
    finan_rept['NetProfExcMin'] = finan_rept['NetProfit'] - finan_rept['MinorityInterest_y']
    finan_rept['TotalShrhldrEquityExcMin'] = finan_rept['TotalShrhldrEquity'] - finan_rept['MinorityInterest_x']
    finan_rept['CashFlow'] = finan_rept['CashEnd'] + finan_rept['CashEquiEnd'] - finan_rept['CashStart'] - finan_rept['CashEquiStart']
    finan_rept.drop(['ShortName_x', 'ReportType_x', 'MonetaryCapital', 'TradingFinAssets', 'TotalCurAssets', 'TotalAssets', 'ShortTermBorrow', 
                    'TradingFinLiab', 'NotesPayable', 'TaxesPayable', 'NonCurLiabIn1Y', 'TotalCurLiab', 'LongTermBorrow', 'MinorityInterest_x', 
                    'TotalShrhldrEquity', 'ShortName_y', 'ReportType_y', 'NetProfit', 'MinorityInterest_y', 'ShortName', 'ReportType', 
                    'DepreciationFaOgaBba', 'AmortizationIntang', 'CashEnd', 'CashStart', 'CashEquiEnd', 'CashEquiStart', 'Stkcd'], axis=1, inplace=True)
    finan_rept.dropna(inplace=True)

    # 上采样到月度，方便寻找离给定月份发布时间最近的财报
    finan_rept_m = finan_rept.groupby(level=0).apply(lambda x: x.droplevel(0).resample('M').ffill())

    # 合并财报数据和交易数据，计算三个价值因子
    TRD_Dalyr = pd.merge(TRD_Dalyr, finan_rept_m, on=['Stkcd', 'Date'], how='inner')
    TRD_Dalyr['EP'] = TRD_Dalyr['NetProfExcMin'] / TRD_Dalyr['MKTValue']
    TRD_Dalyr['BM'] = TRD_Dalyr['TotalShrhldrEquityExcMin'] / TRD_Dalyr['MKTValue']
    TRD_Dalyr['CP'] = TRD_Dalyr['CashFlow'] / TRD_Dalyr['MKTValue']

    # 月return
    df_monthly_return = pd.read_sql('select * from TRD_Mnth;', level1_csmar)
    df_monthly_return['Date'] = df_monthly_return['Date'] + offsets.MonthEnd(0)
    df_monthly_return = df_monthly_return.drop(['MKTValue'], axis=1).drop_duplicates()
    TRD_Dalyr = pd.merge(TRD_Dalyr.drop(['RealPctChange'], axis=1), df_monthly_return, on=['Stkcd', 'Date'], how='left')

    # 删除异常值和缺失值
    TRD_Dalyr.replace([np.inf, -np.inf], np.nan, inplace=True)
    TRD_Dalyr.dropna(inplace=True)
    TRD_Dalyr.reset_index(inplace=True)

    # 保存数据
    if type(df_past) == float:
        return TRD_Dalyr
    else:
        TRD_Dalyr = pd.concat([TRD_Dalyr, df_past]).drop_duplicates(keep=False)
        return TRD_Dalyr

def upload(df_return, df_past):
    
    if type(df_past) == float:
        print(f'[+] {time.strftime("%c")} init Stock_Return_m_Ratio')
        df_return.p_to_sql('Stock_Return_m_Ratio', level2_csmar, partitions=1000, n_workers=12, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(df_return) != 0:
            df_return.p_to_sql('Stock_Return_m_Ratio', level2_csmar, partitions=1, n_workers=1, threads_per_worker=1)
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level2/Stock_Return_m_Ratio')
def Stock_Return_m_Ratio():
    df_past, TRD_Dalyr = read_sql()
    df_return = handle(df_past, TRD_Dalyr)
    upload(df_return, df_past)