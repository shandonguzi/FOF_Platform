import pandas as pd
import numpy as np
from settings.database import *
from utils.mysql.get_sql import get_sql
from scipy.stats import ttest_1samp
from utils.time_function import timeit
import warnings
warnings.filterwarnings('ignore')

# Function to remove outliers using IQR
def remove_outliers_with_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

# 用来计算概率的函数
def calculate_probability_for_factor(factor_name, is_high, start_date, end_date, TRD_Mnth):
    df_factor = get_sql(level3_factors, factor_name)
    df_factor['Date'] = pd.to_datetime(df_factor['Date'])
    df_factor = remove_outliers_with_iqr(df_factor, factor_name)
    df_factor['Date'] = df_factor['Date'] + pd.offsets.MonthEnd(0)
    result_series = pd.Series(index=pd.date_range(start=start_date, end=end_date, freq='M'), dtype=float)
    
    for base_date in result_series.index:
        # 筛选基准月份的因子数据
        base_month_factor = df_factor[df_factor['Date'] == base_date.strftime('%Y-%m-%d')]
        if base_month_factor.empty:  # 如果没有数据，跳过当前月份
            continue
        
        # 选择当前因子的前20%
        threshold = base_month_factor[factor_name].quantile(0.8) if is_high else base_month_factor[factor_name].quantile(0.2)
        if is_high:
            selected_stocks = base_month_factor[base_month_factor[factor_name] >= threshold]['Stkcd']
        else:
            selected_stocks = base_month_factor[base_month_factor[factor_name] <= threshold]['Stkcd']
   
        # 计算未来六个月的时间范围
        monthly_probabilities = []
        for months_ahead in range(1, 7):
            future_date = (base_date + pd.offsets.MonthEnd(months_ahead)).strftime('%Y-%m-%d')

            # 选出未来某月份的收益率数据
            future_month_return = TRD_Mnth[TRD_Mnth['Date'] == future_date]
            future_top_threshold = future_month_return['RealPctChange'].quantile(0.8)
            top_stocks = future_month_return[future_month_return['RealPctChange'] >= future_top_threshold]['Stkcd']
            monthly_probability = len(np.intersect1d(selected_stocks, top_stocks)) / len(top_stocks)
            monthly_probabilities.append(monthly_probability)
        
        # 保存计算得到的平均概率
        if monthly_probabilities:  # 防止除以0的情况
            result_series[base_date] = sum(monthly_probabilities) / len(monthly_probabilities)
    
    return result_series

# 主函数，计算所有因子的平均概率并返回DataFrame
def calculate_factors_probability(factors, start_date, end_date, TRD_Mnth):
    probabilities = {}

    for factor, direction in factors:
        is_high = direction == 'high'  # 如果方向为'high'，则is_high为True
        probabilities[factor] = calculate_probability_for_factor(factor, is_high, start_date, end_date, TRD_Mnth)
        print(f'Finished calculating {factor}.')

    # 将概率字典转换为DataFrame
    return pd.DataFrame(probabilities, index=pd.date_range(start=start_date, end=end_date, freq='M'))


@timeit('Future Returns')
def future_returns():
    TRD_Mnth = get_sql(level1_csmar, 'TRD_Mnth')
    TRD_Mnth['Date'] = TRD_Mnth['Date'] + pd.offsets.MonthEnd(0)
    TRD_Mnth = TRD_Mnth[['Stkcd', 'Date', 'RealPctChange']]

    factors = [('IdioVol', 'high'),  # Trading Friction
               ('Rel2High', 'high'),
               ('ResidVar', 'high'),
               ('AT', 'low'),
               ('CAPM_beta', 'high'),
               ('LME', 'low'),
               ('LTurnover', 'high'),
               ('MktBeta', 'high'),
            #    ('Variance', 'high')]
               ('r2_1', 'high'),  # past return
               ('r12_2', 'high'),
               ('r12_7', 'high'),
               ('r36_13', 'high'),
               ('Investment', 'high'),  # Investment
               ('NOA', 'high'),
               ('DPI2A', 'high'),
               ('NSI', 'high'),
               ('PROF', 'high'),  # Profitability
               ('ATO', 'high'),
               ('CTO', 'high'),
               ('FC2Y', 'high'),
               ('OP', 'high'),
               ('PM', 'high'),
               ('RNA', 'high'),
               ('ROA', 'high'),
               ('ROE', 'high'),
               ('SGA2S', 'high'),
               ('D2A', 'high'),
               ('AC', 'high'),  # Intangibles
               ('OL', 'high'),
               ('PCM', 'high'),
               ('A2ME', 'high'),  # Value
               ('BEME', 'high'),
               ('C', 'high'),
               ('CF', 'high'),
               ('CF2P', 'high'),
               ('E2P', 'high'),
               ('Q', 'high'),
               ('S2P', 'high'),
               ('Lev', 'high'),
    ]

    start_date = '2013-01-31'
    end_date = '2022-12-31'
    result_df = calculate_factors_probability(factors, start_date, end_date, TRD_Mnth)

    means = result_df.mean()
    t_tests = result_df.apply(lambda col: ttest_1samp(col, 0))

    t_values = t_tests.apply(lambda x: x[0])
    p_values = t_tests.apply(lambda x: x[1])
    t_p_df = pd.DataFrame({'mean': means, 't_value': t_values, 'p_value': p_values})

    with pd.ExcelWriter('/code/factors_check/future_return_analysis.xlsx') as writer:  
        result_df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0)
        t_p_df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=len(result_df.columns) + 2)
future_returns()