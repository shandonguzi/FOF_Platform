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

def get_highfreqEcoGrowth(all_factors):

    all_factors_filled = all_factors.apply(fill_missing_values)
    all_factors_filled = all_factors_filled.fillna(method='ffill')
    factors_YoYRet_M = all_factors_filled.resample('M').last().pct_change(12).dropna()*100
    factors_YoYRet_W = all_factors_filled.resample('W-FRI').last().pct_change(52).dropna()*100
    factors_WoWRet_W = all_factors_filled.resample('W-FRI').last().pct_change(1).dropna()*100

    x11 = factors_YoYRet_M['HSI']
    x12 = factors_YoYRet_M['CRBSpotMetal']
    y1 = get_sql(level3_factors, 'lowfreq_EcoGrowth').set_index('Date')
    y1.index = pd.to_datetime(y1.index)
    
    weights_EcoGrowth = get_weights(1, y1, x11, x12)
    weights_EcoGrowth.index = weights_EcoGrowth.index + pd.offsets.MonthEnd(1)

    weights_EcoGrowth_week = weights_EcoGrowth.resample('W-FRI').ffill()
    df_combined1 = pd.concat([factors_YoYRet_W['HSI'], factors_YoYRet_W['CRBSpotMetal'], weights_EcoGrowth_week], axis=1, join='inner')
    HighFreqEcoGrowth_YoY = df_combined1['weight_HSI'] * df_combined1['HSI'] + df_combined1['weight_CRBSpotMetal'] * df_combined1['CRBSpotMetal'] 
    HighFreqEcoGrowth_YoY.name = 'HighFreqEcoGrowth_YoY'

    df_combinedWoW1 = pd.concat([factors_WoWRet_W['HSI'], factors_WoWRet_W['CRBSpotMetal'], weights_EcoGrowth_week], axis=1, join='inner')
    HighFreqEcoGrowth_WoW = df_combinedWoW1['weight_HSI'] * df_combinedWoW1['HSI'] + df_combinedWoW1['weight_CRBSpotMetal'] * df_combinedWoW1['CRBSpotMetal']
    HighFreqEcoGrowth_WoW.name = 'HighFreqEcoGrowth_WoW'

    return HighFreqEcoGrowth_YoY, HighFreqEcoGrowth_WoW


def update(HighFreqEcoGrowth_YoY, HighFreqEcoGrowth_WoW):
    HighFreqEcoGrowth_YoY = HighFreqEcoGrowth_YoY.reset_index()
    HighFreqEcoGrowth_WoW = HighFreqEcoGrowth_WoW.reset_index()
    HighFreqEcoGrowth_YoY.to_sql('HighFreqEcoGrowth_YoY', con=level3_factors, if_exists='replace', index=False)
    HighFreqEcoGrowth_WoW.to_sql('HighFreqEcoGrowth_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/highfreq_EcoGrowth')
def highfreq_EcoGrowth():
    HSI = read_sql('HSI')
    CRBSpotMetal = read_sql('CRBSpotMetal')
    all_factors = pd.concat([HSI, CRBSpotMetal], axis=1)
    HighFreqEcoGrowth_YoY, HighFreqEcoGrowth_WoW = get_highfreqEcoGrowth(all_factors)
    update(HighFreqEcoGrowth_YoY, HighFreqEcoGrowth_WoW)

if __name__ == '__main__':
    highfreq_EcoGrowth()
