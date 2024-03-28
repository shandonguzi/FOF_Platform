import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from dateutil.relativedelta import relativedelta
import statsmodels.api as sm
import warnings
import matplotlib.ticker as mtick

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


warnings.filterwarnings('ignore')



def timechoose(df, start, end=None):
    df = df[(df.index >= start) & (df.index <= end)] if end else df[df.index >= start]
    return df


# 填补缺失值
def fill_missing_values(column):
    # 根据过去12个月的一阶差分中位数来填充列中的缺失值。
    diff_series = column.diff()
    diff_median12 = diff_series.shift(1).rolling(window=12, min_periods=1).median()
    fill_values = column.copy()
    missing_indexes = column[column.isna()].index
    if not missing_indexes.empty:
        if missing_indexes[0] == column.index[0]:
            missing_indexes = missing_indexes.drop(missing_indexes[0])
            for idx in missing_indexes:
                # 首先判断前面有没有非nan值，如果没有，就在missing_indexes中删除该值，继续对下一个值进行判断
                idx_pos = column.index.get_loc(idx)
                if pd.isna(column.iloc[idx_pos - 1]):
                    missing_indexes = missing_indexes.drop(idx)
                    continue
                else:
                    break
        for idx in missing_indexes:
            idx_pos = column.index.get_loc(idx)
            fill_values[idx] = (fill_values.iloc[idx_pos - 1] + diff_median12.loc[idx])
            # fill_values[idx] = fill_values.iloc[idx_pos - 1] if pd.isna(column.iloc[idx_pos - 1]) else column.iloc[idx_pos - 1] + diff_median12.loc[idx]

    return column.fillna(fill_values)


# 基于同比数据调整绝对值
def adjust_absolute_value(column, columnYoY, base_year):    

    adjusted_values = pd.Series(index=column.index, dtype=float)
    for month in column[column.index.year == base_year].index:
        adjusted_values[month] = column[month]   
    # 处理基年之后的数据（向后调整）
    for year in range(base_year + 1, column.index.year.max() + 1):
        for month in column[column.index.year == year].index.month:
            # 计算新的绝对值
            prev_year_value = adjusted_values.loc[pd.Timestamp(year=year-1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = columnYoY.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            adjusted_values.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = prev_year_value * (1 + yoy_growth)
    # 处理基年之前的数据（向前调整）
    for year in range(base_year - 1, column.index.year.min() - 1, -1):
        for month in column[column.index.year == year].index.month:
            # 计算新的绝对值
            next_year_value = adjusted_values.loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = columnYoY.loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            adjusted_values.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = next_year_value / (1 + yoy_growth)

    return adjusted_values


# 合成经济增长同比:将工业增加值同比、PMI同比差分、社会消费品零售总额同比以滚动24个月波动率倒数加权的方式合成经济增长同比
def cal_EcoGrowth(all_factors_hp): 

    all_factors_hp['24Volatility_IndustrialAddedValueIndex'] = all_factors_hp['IndustrialAddedValueIndex'].rolling(window=24).std()
    all_factors_hp['24Volatility_PMI'] = all_factors_hp['PMI'].rolling(window=24).std()
    all_factors_hp['24Volatility_TotalRetailSales'] = all_factors_hp['AdjTotalRetailSales'].rolling(window=24).std() 
    all_factors_hp['Weight_IndustrialAddedValueIndex'] = 1 / (all_factors_hp['24Volatility_IndustrialAddedValueIndex']) 
    all_factors_hp['Weight_PMI'] = 1 / (all_factors_hp['24Volatility_PMI'])
    all_factors_hp['Weight_TotalRetailSales'] = 1 / (all_factors_hp['24Volatility_TotalRetailSales'])
    all_factors_hp['denom'] = all_factors_hp['Weight_IndustrialAddedValueIndex'] + all_factors_hp['Weight_PMI'] + all_factors_hp['Weight_TotalRetailSales']
    all_factors_hp['EcoGrowth'] = all_factors_hp['IndustrialAddedValueIndex'] * all_factors_hp['Weight_IndustrialAddedValueIndex'] / all_factors_hp['denom'] + \
                                all_factors_hp['PMI'] * all_factors_hp['Weight_PMI'] / all_factors_hp['denom'] + \
                                all_factors_hp['AdjTotalRetailSales'] * all_factors_hp['Weight_TotalRetailSales'] / all_factors_hp['denom']

    return all_factors_hp['EcoGrowth']


# (3)计算资产权重:以领先期 n 带入回归模型，第 i 月月底以 i-24 个月到第 i 月的低频宏观因子为因变量，
# i-24-n 到 i-n 的资产同比收益率为自变量，构建多元领先回归模型，确定第 i+1 月用于合成高频宏观因子的资产权重。
def get_weights(n, y, *args):
    y=y.dropna()
    args = [x.dropna() for x in args]
    # 确保x和y的起始日期相同
    start_date = max([x.index[0] for x in args] + [y.index[0]])
    y = y[y.index >= start_date]
    args = [x[x.index >= start_date] for x in args]
    weights = []
    shifted_series = [x for x in args]
    totalX = pd.concat(shifted_series, axis=1)
    for i in range(n+24+1, len(y)):
        x_ = totalX[(i-24-n-1):(i-n)].values
        y_ = y[i-24-1:i].values
        x_ = sm.add_constant(x_)
        model = sm.OLS(y_, x_).fit()
        weights.append(model.params[1:])
    weights = pd.DataFrame(weights, columns=totalX.columns)
    weights.columns = 'weight_' + weights.columns
    weights.index = y.index[24+n+1:]
    return weights





