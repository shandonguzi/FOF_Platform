
import pandas as pd
import statsmodels.api as sm
from scipy import stats
from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
import warnings
warnings.filterwarnings('ignore')


def remove_outliers_with_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    return df[(df[column] >= (Q1 - 1.5 * IQR)) & (df[column] <= (Q3 + 1.5 * IQR))]


def calculate_returns(group, n_quantiles, is_high):
    group['quantile'] = pd.qcut(group[group.columns[2]].rank(method='first'), n_quantiles, labels=False, duplicates='drop')
    # 计算长仓和短仓股票的平均收益率
    if is_high == 'high':
        long_funds = group[group['quantile'] == (n_quantiles - 1)]['Symbol']
        median_funds = group[group['quantile'] == round((n_quantiles - 1)/2)]['Symbol']
        short_funds = group[group['quantile'] == 0]['Symbol']
    else:
        long_funds = group[group['quantile'] == 0]['Symbol']
        median_funds = group[group['quantile'] == round((n_quantiles - 1)/2)]['Symbol']
        short_funds = group[group['quantile'] == (n_quantiles - 1)]['Symbol']
    long_return = group[group['Symbol'].isin(long_funds)]['Fund_Return_m'].mean()
    short_return = group[group['Symbol'].isin(short_funds)]['Fund_Return_m'].mean()
    median_return = group[group['Symbol'].isin(median_funds)]['Fund_Return_m'].mean()
    return pd.Series({
        'Long_Return': long_return,
        'Median_Return': median_return,
        'Short_Return': short_return,
        'Long_Short_Return': long_return - short_return,
    })


def capm_svc(factor_data, month_return, svc, n_quantiles, time_start, time_end=None, direction='high'):
    # Merge and filter the data
    merged_data = pd.merge(factor_data, month_return, on=['Date', 'Symbol'], how='inner')
    merged_data = merged_data[(merged_data.Date >= time_start) & (merged_data.Date <= time_end)] if time_end else merged_data[merged_data.Date >= time_start]

    # Calculate returns for quantile groups
    returns = merged_data.groupby('Date').apply(calculate_returns, n_quantiles=n_quantiles, is_high=direction)
    returns = pd.merge(returns, svc, on='Date', how='inner')
    
    # CAPM
    long_x = sm.add_constant(returns[['mktrf']])
    long_y = returns['Long_Return'] - returns['rf']
    long_model = sm.OLS(long_y, long_x).fit()
    long_alpha, _ = long_model.params
    long_alpha_t = long_model.tvalues[0]
    long_r2 = long_model.rsquared
    long_excess = long_y.mean()
    long_excess_t = stats.ttest_1samp(long_y, 0)[0]

    short_x = sm.add_constant(returns[['mktrf']])
    short_y = returns['Short_Return'] - returns['rf']
    short_model = sm.OLS(short_y, short_x).fit()
    short_alpha, _ = short_model.params
    short_alpha_t = short_model.tvalues[0]
    short_r2 = short_model.rsquared
    short_excess = short_y.mean()
    short_excess_t = stats.ttest_1samp(short_y, 0)[0]

    median_x = sm.add_constant(returns[['mktrf']])
    median_y = returns['Median_Return'] - returns['rf']
    median_model = sm.OLS(median_y, median_x).fit()
    median_alpha, _ = median_model.params
    median_alpha_t = median_model.tvalues[0]
    median_r2 = median_model.rsquared
    median_excess = median_y.mean()
    median_excess_t = stats.ttest_1samp(median_y, 0)[0]

    long_short_x = sm.add_constant(returns[['mktrf']])
    long_short_y = returns['Long_Short_Return']
    long_short_model = sm.OLS(long_short_y, long_short_x).fit()
    long_short_alpha, _ = long_short_model.params
    long_short_alpha_t = long_short_model.tvalues[0]
    long_short_r2 = long_short_model.rsquared
    long_short_excess = long_short_y.mean()
    long_short_excess_t = stats.ttest_1samp(long_short_y, 0)[0]

    CAPM_results = pd.DataFrame({
        'Long': [long_excess, long_excess_t, long_alpha, long_alpha_t, long_r2],
        'Short': [short_excess, short_excess_t, short_alpha, short_alpha_t, short_r2],
        'Median': [median_excess, median_excess_t, median_alpha, median_alpha_t, median_r2],
        'Long_Short': [long_short_excess, long_short_excess_t, long_short_alpha, long_short_alpha_t, long_short_r2]
    }, index=['excess', 'excess_t', 'capm_α', 'capm_α_t', 'capm_R2'])

    # SVC
    long_x = sm.add_constant(returns[['mktrf', 'smb', 'vmg']])
    long_y = returns['Long_Return'] - returns['rf']
    long_model = sm.OLS(long_y, long_x).fit()
    long_alpha, _, _, _ = long_model.params
    long_alpha_t = long_model.tvalues[0]
    long_r2 = long_model.rsquared

    short_x = sm.add_constant(returns[['mktrf', 'smb', 'vmg']])
    short_y = returns['Short_Return'] - returns['rf']
    short_model = sm.OLS(short_y, short_x).fit()
    short_alpha, _, _, _ = short_model.params
    short_alpha_t = short_model.tvalues[0]
    short_r2 = short_model.rsquared

    median_x = sm.add_constant(returns[['mktrf', 'smb', 'vmg']])
    median_y = returns['Median_Return'] - returns['rf']
    median_model = sm.OLS(median_y, median_x).fit()
    median_alpha, _, _, _ = median_model.params
    median_alpha_t = median_model.tvalues[0]
    median_r2 = median_model.rsquared

    long_short_x = sm.add_constant(returns[['mktrf', 'smb', 'vmg']])
    long_short_y = returns['Long_Short_Return'] - returns['rf']
    long_short_model = sm.OLS(long_short_y, long_short_x).fit()
    long_short_alpha, _, _, _ = long_short_model.params
    long_short_alpha_t = long_short_model.tvalues[0]
    long_short_r2 = long_short_model.rsquared
    
    SVC_results = pd.DataFrame({
        'Long': [long_alpha, long_alpha_t, long_r2],
        'Short': [short_alpha, short_alpha_t, short_r2],
        'Median': [median_alpha, median_alpha_t, median_r2],
        'Long_Short': [long_short_alpha, long_short_alpha_t, long_short_r2]
    }, index=['svc_α', 'svc_α_t', 'svc_R2'])
    
    result = pd.concat([CAPM_results, SVC_results], axis=0)
    result.loc['excess', :] = result.loc['excess', :]*100
    result.loc['capm_α', :] = result.loc['capm_α', :]*100
    result.loc['svc_α', :] = result.loc['svc_α', :]*100
    return result.round(3)


@timeit('capm_svc_check')
def capm_svc_check():
    # 自定义参数：分组数、起始时间、结束时间、factors及其方向
    n_quantiles = 5
    time_start = '2010-01-01'
    # time_end = 
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
    svc = get_sql(level0_jiayin, 'm_CH3')

    analysis_results = {}

    for factor, direction in factors:
        factor_data = get_sql(level3_factors, factor)
        factor_data['Date'] = pd.to_datetime(factor_data['Date']) + pd.offsets.MonthEnd(2)
        factor_data = factor_data.groupby('Date').apply(remove_outliers_with_iqr, factor_data.columns[2]).reset_index(drop=True)
        analysis_results[factor] = capm_svc(factor_data, Fund_Return_m, svc, n_quantiles, time_start, direction=direction)
        print(f'Finished analyzing factor: {factor}')
    with pd.ExcelWriter('/code/factors_check/capm_svc_check.xlsx') as writer:
        row = 0  # 初始行数
        col = 0  # 初始列数
        factor_count = 0  # 因子计数器

        for factor_name, df in analysis_results.items():
            # 写入因子名
            pd.DataFrame([f"{factor_name}"]).to_excel(writer, sheet_name='Sheet1', startrow=row, startcol=col, index=False, header=False)
            
            # 写入因子数据
            df.to_excel(writer, sheet_name='Sheet1', startrow=row+1, startcol=col, index=True, header=True)

            factor_count += 1
            col += len(df.columns) + 2  # 更新列位置

            # 每三个因子换行
            if factor_count % 3 == 0:
                row += len(df) + 3  # 更新行位置
                col = 0  # 重置列位置
capm_svc_check()