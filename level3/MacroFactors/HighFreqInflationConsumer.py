import pandas as pd
import numpy as np
import statsmodels.api as sm
import warnings

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from MactorFactors.MF_level3.cal_highfreq_MF import *


warnings.filterwarnings('ignore')

def read_sql(FactorName):
    df = get_sql(level0_wind, FactorName)
    df = df.set_index('Date')
    df.index = pd.to_datetime(df.index)
    df = timechoose(df, '2010-01-01')
    return df

def get_highfreqInfConsumer(all_factors):

    all_factors_filled = all_factors.apply(fill_missing_values)
    all_factors_filled = all_factors_filled.fillna(method='ffill')
    factors_YoYRet_M = all_factors_filled.resample('M').last().pct_change(12).dropna()*100
    factors_YoYRet_W = all_factors_filled.resample('W-FRI').last().pct_change(52).dropna()*100
    factors_WoWRet_W = all_factors_filled.resample('W-FRI').last().pct_change(1).dropna()*100

    x21 = factors_YoYRet_M['AgriPriceIdx']
    x22 = factors_YoYRet_M['PorkPrice']
    y2 = get_sql(level3_factors, 'lowfreq_InfConsumer').set_index('Date')
    y2.index = pd.to_datetime(y2.index)
    
    weights_InfConsumer = get_weights(2, y2, x21, x22)
    weights_InfConsumer.index = weights_InfConsumer.index + pd.offsets.MonthEnd(1)

    weights_InfConsumer_week = weights_InfConsumer.resample('W-FRI').ffill()
    df_combined2 = pd.concat([factors_YoYRet_W['AgriPriceIdx'], factors_YoYRet_W['PorkPrice'], weights_InfConsumer_week], axis=1, join='inner')
    HighFreqInflationConsumer_YoY = df_combined2['weight_AgriPriceIdx'] * df_combined2['AgriPriceIdx'] + df_combined2['weight_PorkPrice'] * df_combined2['PorkPrice']
    HighFreqInflationConsumer_YoY.name = 'HighFreqInflationConsumer_YoY'

    df_combinedWoW2 = pd.concat([factors_WoWRet_W['AgriPriceIdx'], factors_WoWRet_W['PorkPrice'], weights_InfConsumer_week], axis=1, join='inner')
    HighFreqInflationConsumer_WoW = df_combinedWoW2['weight_AgriPriceIdx'] * df_combinedWoW2['AgriPriceIdx'] + df_combinedWoW2['weight_PorkPrice'] * df_combinedWoW2['PorkPrice']
    HighFreqInflationConsumer_WoW.name = 'HighFreqInflationConsumer_WoW'

    return HighFreqInflationConsumer_YoY, HighFreqInflationConsumer_WoW
    

def update(HighFreqInflationConsumer_YoY, HighFreqInflationConsumer_WoW):
    HighFreqInflationConsumer_YoY = HighFreqInflationConsumer_YoY.reset_index()
    HighFreqInflationConsumer_WoW = HighFreqInflationConsumer_WoW.reset_index()
    HighFreqInflationConsumer_YoY.to_sql('HighFreqInflationConsumer_YoY', con=level3_factors, if_exists='replace', index=False)
    HighFreqInflationConsumer_WoW.to_sql('HighFreqInflationConsumer_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/HighFreqInflationConsumer')
def HighFreqInflationConsumer():
    AgriPriceIdx = read_sql('AgriPriceIdx')
    PorkPrice = read_sql('PorkPrice')
    all_factors = pd.concat([AgriPriceIdx, PorkPrice], axis=1)
    HighFreqInflationConsumer_YoY, HighFreqInflationConsumer_WoW = get_highfreqInfConsumer(all_factors)
    update(HighFreqInflationConsumer_YoY, HighFreqInflationConsumer_WoW)

if __name__ == '__main__':
    HighFreqInflationConsumer()