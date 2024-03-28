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
    return df[(df[column] >= (Q1 - 1.5 * IQR)) & (df[column] <= (Q3 + 1.5 * IQR))]

# 用来计算概率的函数
def calculate_probability_for_factor(factor_name, is_high, start_date, end_date, Fund_Return_m, quantile):
    df_factor = get_sql(level3_factors, factor_name)
    df_factor['Date'] = pd.to_datetime(df_factor['Date'])
    df_factor = df_factor.groupby('Date').apply(remove_outliers_with_iqr, df_factor.columns[2]).reset_index(drop=True)
    df_factor['Date'] = df_factor['Date'] + pd.offsets.MonthEnd(0)
    result_series = pd.Series(index=pd.date_range(start=start_date, end=end_date, freq='M'), dtype=float)
    
    for base_date in result_series.index:
        # 筛选基准月份的因子数据
        base_month_factor = df_factor[df_factor['Date'] == base_date.strftime('%Y-%m-%d')]
        if base_month_factor.empty:  # 如果没有数据，跳过当前月份
            continue
        
        # 选择当前因子排名的前20%
        threshold = base_month_factor[df_factor.columns[2]].quantile(1-quantile) if is_high else base_month_factor[df_factor.columns[2]].quantile(quantile)
        if is_high:
            selected_funds = base_month_factor[base_month_factor[df_factor.columns[2]] >= threshold]['Symbol']
        else:
            selected_funds = base_month_factor[base_month_factor[df_factor.columns[2]] <= threshold]['Symbol']
   
        # 计算未来六个月的时间范围
        monthly_probabilities = []
        for months_ahead in range(1, 7):
            future_date = (base_date + pd.offsets.MonthEnd(months_ahead)).strftime('%Y-%m-%d')

            # 选出未来某月份的收益率数据
            future_month_return = Fund_Return_m[Fund_Return_m['Date'] == future_date]
            future_top_threshold = future_month_return['Fund_Return_m'].quantile(1-quantile)
            top_funds = future_month_return[future_month_return['Fund_Return_m'] >= future_top_threshold]['Symbol']
            monthly_probability = len(np.intersect1d(selected_funds, top_funds)) / len(top_funds)
            monthly_probabilities.append(monthly_probability)
        
        # 保存计算得到的平均概率
        if monthly_probabilities:  # 防止除以0的情况
            result_series[base_date] = sum(monthly_probabilities) / len(monthly_probabilities)
    
    result_series = result_series.round(3)
    return result_series

# 主函数，计算所有因子的平均概率并返回DataFrame
def calculate_factors_probability(factors, start_date, end_date, Fund_Return_m, quantile):
    probabilities = {}

    for factor, direction in factors:
        is_high = direction == 'high'  # 如果方向为'high'，则is_high为True
        probabilities[factor] = calculate_probability_for_factor(factor, is_high, start_date, end_date, Fund_Return_m, quantile)
        print(f'[+] Finished calculating {factor}')

    # 将概率字典转换为DataFrame
    return pd.DataFrame(probabilities, index=pd.date_range(start=start_date, end=end_date, freq='M'))


@timeit('Future Returns')
def future_returns():
    # 自定义参数：起始时间、结束时间、factors及其方向、未来六个月top_return分位数(0.2为前20%)
    start_date = '2010-01-31'
    end_date = '2022-12-31'
    quantile = 0.1
    factors = [('capm_α', 'high'),  # now factors
               ('svc_α', 'high'),
               ('capm_α_ε', 'high'),
               ('svc_α_ε', 'high'),
               ('industry_concentration', 'high'),
               ('connected_companies_portfolio_capm', 'high'),
               ('connected_companies_portfolio_svc', 'high'),
               ('active_share', 'high'),
               ('return_gap', 'high'),
               ('ME', 'low'),
               ('TD_α', 'high'),
               ('TDP_α', 'high'),
               ('Fund_IdioVol', 'high'),  # Trading Friction
               ('Fund_Rel2High', 'high'),
               ('Fund_ResidVar', 'high'),
               ('Fund_AT', 'low'),
               ('Fund_CAPM_beta', 'high'),
               ('Fund_LME', 'low'),
               ('Fund_LTurnover', 'high'),
               ('Fund_MktBeta', 'high'),
            #    ('Variance', 'high')]
               ('Fund_r2_1', 'high'),  # past return
               ('Fund_r12_2', 'high'),
               ('Fund_r12_7', 'high'),
               ('Fund_r36_13', 'high'),
               ('Fund_Investment', 'high'),  # Investment
               ('Fund_NOA', 'high'),
               ('Fund_DPI2A', 'high'),
               ('Fund_NSI', 'high'),
               ('Fund_PROF', 'high'),  # Profitability
               ('Fund_ATO', 'high'),
               ('Fund_CTO', 'high'),
               ('Fund_FC2Y', 'high'),
               ('Fund_OP', 'high'),
               ('Fund_PM', 'high'),
               ('Fund_RNA', 'high'),
               ('Fund_ROA', 'high'),
               ('Fund_ROE', 'high'),
               ('Fund_SGA2S', 'high'),
               ('Fund_D2A', 'high'),
               ('Fund_AC', 'high'),  # Intangibles
               ('Fund_OL', 'high'),
               ('Fund_PCM', 'high'),
               ('Fund_A2ME', 'high'),  # Value
               ('Fund_BEME', 'high'),
               ('Fund_C', 'high'),
               ('Fund_CF', 'high'),
               ('Fund_CF2P', 'high'),
               ('Fund_E2P', 'high'),
               ('Fund_Q', 'high'),
               ('Fund_S2P', 'high'),
               ('Fund_Lev', 'high'),
               ('Fund_age', 'low'),  # FundCharacters
               ('Fund_tna', 'low'),
               ('F_r12_2', 'high'),  # FundMomentum
               ('F_ST_Rev', 'high')
    ]

    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m')
    Fund_Return_m = Fund_Return_m[['Date', 'Symbol', 'Fund_Return_m']]

    result_df = calculate_factors_probability(factors, start_date, end_date, Fund_Return_m, quantile)

    means = result_df.mean()
    t_tests = result_df.apply(lambda col: ttest_1samp(col, quantile))

    t_values = t_tests.apply(lambda x: x[0])
    p_values = t_tests.apply(lambda x: x[1])
    t_p_df = pd.DataFrame({'mean': means, 't_value': t_values, 'p_value': p_values})

    with pd.ExcelWriter('/code/factors_check/future_return_analysis.xlsx') as writer:  
        result_df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0)
        t_p_df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=len(result_df.columns) + 2)
future_returns()