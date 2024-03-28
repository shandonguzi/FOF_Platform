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


def get_lowfreqEcoGrowth(all_factors):
    
    all_factors_filled = all_factors.apply(fill_missing_values)
    # 对于TotalRetailSales，使用2011年各月的当月值作为基年，使用同比值向前和向后进行调整
    all_factors_filled['AdjTotalRetailSales'] = adjust_absolute_value(all_factors_filled['TotalRetailSales'], all_factors_filled['TotalRetailSalesYoY'], 2011)
    # 用IndustrialAddedValueIndexYoY填补IndustrialAddedValueIndex的缺失值
    first_valid_index = all_factors_filled['IndustrialAddedValueIndex'].first_valid_index()
    for year in range(first_valid_index.year - 1, all_factors_filled.index.year.min() - 1, -1):
        for month in all_factors_filled[all_factors_filled.index.year == year].index.month:
            next_year_value = all_factors_filled['IndustrialAddedValueIndex'].loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = all_factors_filled['IndustrialAddedValueIndexYoY'].loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            all_factors_filled['IndustrialAddedValueIndex'].loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = next_year_value / (1 + yoy_growth)
    # 季节性调整
    all_factors_seasadj = pd.DataFrame()
    all_factors_seasadj['IndustrialAddedValueIndex'] = sm.tsa.x13_arima_analysis(all_factors_filled['IndustrialAddedValueIndex'],
                                                                                trading=True,
                                                                                x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['PMI'] = sm.tsa.x13_arima_analysis(all_factors_filled['PMI'],
                                                        trading=True,
                                                        x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['AdjTotalRetailSales'] = sm.tsa.x13_arima_analysis(all_factors_filled['AdjTotalRetailSales'],
                                                                        trading=True,
                                                                        x12path="/code/x13as/x13as_ascii").seasadj
    # 计算同比值,对于PMI 则计算同比差分
    all_factors_seasadj['IndustrialAddedValueIndexYoY'] = all_factors_seasadj['IndustrialAddedValueIndex'].pct_change(periods=12) * 100
    all_factors_seasadj['PMIYoY'] = all_factors_seasadj['PMI'].diff(periods=12)
    all_factors_seasadj['AdjTotalRetailSalesYoY'] = all_factors_seasadj['AdjTotalRetailSales'].pct_change(periods=12) * 100
    # 单向 hp 滤波处理
    all_factors_hp = pd.DataFrame()
    all_factors_hp['IndustrialAddedValueIndex'] = sm.tsa.filters.hpfilter(all_factors_seasadj['IndustrialAddedValueIndexYoY'].dropna(), lamb=1)[1]
    all_factors_hp['PMI'] = sm.tsa.filters.hpfilter(all_factors_seasadj['PMIYoY'].dropna(), lamb=1)[1]
    all_factors_hp['AdjTotalRetailSales'] = sm.tsa.filters.hpfilter(all_factors_seasadj['AdjTotalRetailSalesYoY'].dropna(), lamb=1)[1]
    # 数据偏移
    all_factors_hp['IndustrialAddedValueIndex'] = all_factors_hp['IndustrialAddedValueIndex'].shift(1)
    all_factors_hp['AdjTotalRetailSales'] = all_factors_hp['AdjTotalRetailSales'].shift(1)
    all_factors_hp = all_factors_hp.dropna()
    # 合成经济增长同比:将工业增加值同比、PMI同比差分、社会消费品零售总额同比以滚动24个月波动率倒数加权的方式合成经济增长同比
    lowfreq_EcoGrowth = cal_EcoGrowth(all_factors_hp)
    lowfreq_EcoGrowth = lowfreq_EcoGrowth.dropna()

    return lowfreq_EcoGrowth


def update(data):
    data = data.reset_index()
    data.to_sql('lowfreq_EcoGrowth', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/lowfreq_EcoGrowth')
def lowfreq_EcoGrowth():
    IndustrialAddedValueIndex = read_sql('IndustrialAddedValueIndex')
    IndustrialAddedValueIndexYoY = read_sql('IndustrialAddedValueIndexYoY')
    PMI = read_sql('PMI')
    TotalRetailSales = read_sql('TotalRetailSales')
    TotalRetailSalesYoY = read_sql('TotalRetailSalesYoY')
    all_factors = pd.concat([IndustrialAddedValueIndex, IndustrialAddedValueIndexYoY, PMI, TotalRetailSales, TotalRetailSalesYoY], axis=1)
    lowfreq_EcoGrowth = get_lowfreqEcoGrowth(all_factors)
    update(lowfreq_EcoGrowth)


if __name__ == '__main__':
    lowfreq_EcoGrowth()
    