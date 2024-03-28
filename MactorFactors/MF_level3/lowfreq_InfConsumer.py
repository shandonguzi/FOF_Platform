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


def get_lowfreqInfConsumer(all_factors):
    
    all_factors_filled = all_factors.apply(fill_missing_values)
    # 使用2011年各月的当月值作为基年，使用同比值向前和向后进行调整
    all_factors_filled['AdjCPI'] = pd.Series(index=all_factors_filled.index, dtype=float)
    all_factors_filled['AdjCPI'] = all_factors_filled['AdjCPI'].fillna(1)
    all_factors_filled['AdjCPI'] = adjust_absolute_value(all_factors_filled['AdjCPI'], all_factors_filled['CPI'], 2011)
    # 季节性调整
    all_factors_seasadj = pd.DataFrame()
    all_factors_seasadj['AdjCPI'] = sm.tsa.x13_arima_analysis(all_factors_filled['AdjCPI'],
                                                            trading=True,
                                                            x12path="/code/x13as/x13as_ascii").seasadj
    # 同比
    all_factors_seasadj['AdjCPIYoY'] = all_factors_seasadj['AdjCPI'].pct_change(periods=12) * 100
    # 单向 hp 滤波处理
    all_factors_hp = pd.DataFrame()
    all_factors_hp['AdjCPI'] = sm.tsa.filters.hpfilter(all_factors_seasadj['AdjCPIYoY'].dropna(), lamb=1)[1]

    return all_factors_hp['AdjCPI']


def update(data):
    data = data.reset_index()
    data.to_sql('lowfreq_InfConsumer', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/lowfreq_InfConsumer')
def lowfreq_InfConsumer():
    CPI = read_sql('CPI')
    CPI = CPI.resample('M').last()
    lowfreq_InfConsumer = get_lowfreqInfConsumer(CPI)
    update(lowfreq_InfConsumer)


if __name__ == '__main__':
    lowfreq_InfConsumer()
    