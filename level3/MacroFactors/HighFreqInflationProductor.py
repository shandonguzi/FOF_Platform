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

    x31 = factors_YoYRet_M['ProdMatIdx']
    x32 = factors_YoYRet_M['CRBSpotIndMat']
    x33 = factors_YoYRet_M['CRBSpotComposite']
    y3 = get_sql(level3_factors, 'lowfreq_InfProductor').set_index('Date')
    y3.index = pd.to_datetime(y3.index)
    
    weights3_InfProductor = get_weights(4, y3, x31, x32, x33)
    weights3_InfProductor.index = weights3_InfProductor.index + pd.offsets.MonthEnd(1)

    weights_InfProductor_week = weights3_InfProductor.resample('W-FRI').ffill()
    df_combined3 = pd.concat([factors_YoYRet_W['ProdMatIdx'], factors_YoYRet_W['CRBSpotIndMat'], factors_YoYRet_W['CRBSpotComposite'], weights_InfProductor_week], axis=1, join='inner')
    HighFreqInflationProductor_YoY = df_combined3['weight_ProdMatIdx'] * df_combined3['ProdMatIdx'] + df_combined3['weight_CRBSpotIndMat'] * df_combined3['CRBSpotIndMat'] + df_combined3['weight_CRBSpotComposite'] * df_combined3['CRBSpotComposite']
    HighFreqInflationProductor_YoY.name = 'HighFreqInflationProductor_YoY'

    df_combinedWoW3 = pd.concat([factors_WoWRet_W['ProdMatIdx'], factors_WoWRet_W['CRBSpotIndMat'], factors_WoWRet_W['CRBSpotComposite'], weights_InfProductor_week], axis=1, join='inner')
    HighFreqInflationProductor_WoW = df_combinedWoW3['weight_ProdMatIdx'] * df_combinedWoW3['ProdMatIdx'] + df_combinedWoW3['weight_CRBSpotIndMat'] * df_combinedWoW3['CRBSpotIndMat'] + df_combinedWoW3['weight_CRBSpotComposite'] * df_combinedWoW3['CRBSpotComposite']
    HighFreqInflationProductor_WoW.name = 'HighFreqInflationProductor_WoW'

    return HighFreqInflationProductor_YoY, HighFreqInflationProductor_WoW


def update(HighFreqInflationProductor_YoY, HighFreqInflationProductor_WoW):
    HighFreqInflationProductor_YoY = HighFreqInflationProductor_YoY.reset_index()
    HighFreqInflationProductor_WoW = HighFreqInflationProductor_WoW.reset_index()
    HighFreqInflationProductor_YoY.to_sql('HighFreqInflationProductor_YoY', con=level3_factors, if_exists='replace', index=False)
    HighFreqInflationProductor_WoW.to_sql('HighFreqInflationProductor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/HighFreqInflationProductor')
def HighFreqInflationProductor():
    ProdMatIdx = read_sql('ProdMatIdx')
    CRBSpotIndMat = read_sql('CRBSpotIndMat')
    CRBSpotComposite = read_sql('CRBSpotComposite')
    all_factors = pd.concat([ProdMatIdx, CRBSpotIndMat, CRBSpotComposite], axis=1)
    HighFreqInflationProductor_YoY, HighFreqInflationProductor_WoW = get_highfreqInfConsumer(all_factors)
    update(HighFreqInflationProductor_YoY, HighFreqInflationProductor_WoW)


if __name__ == '__main__':
    HighFreqInflationProductor()