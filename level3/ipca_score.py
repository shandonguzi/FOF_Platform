
import time
import pandas as pd
from ipca import InstrumentedPCA
import numpy as np
from datetime import datetime
from functools import reduce

from utils.frequent_dates import *
from utils.time_function import timeit
from utils.mysql.get_sql import get_sql
from settings.database import level2_csmar, level3_factors, level1_csmar, level0_joinquant




# 把时间戳转换为int
def convert_date_to_int(date_period):
    date_str = str(date_period)
    date_obj = datetime.strptime(date_str, '%Y-%m')
    return int(date_obj.strftime('%Y%m'))


# 生成Fund_Allocation_filter，用于筛选基金类型
def read_sql():

    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    
    return Classification


def get_filtered_risky_symbol(Classification):

    stock_symbol = Classification[Classification.underlying_asset_type == '股票型'].Symbol.values
    blend_symbol = Classification[Classification.underlying_asset_type == '混合型'].Symbol.values
    risky_symbol = np.append(blend_symbol, stock_symbol)
    # risky_symbol = stock_symbol
    open_fund_filter = Classification[Classification.operate_mode == '开放式基金'].Symbol.values
    one_year_filter = Classification[(pd.Timestamp('now') - Classification.start_date) > pd.Timedelta(days=365)].Symbol.values

    filtered_risky_symbol = reduce(np.intersect1d, (risky_symbol, open_fund_filter, one_year_filter))

    return filtered_risky_symbol


def get_Fund_Allocation_filter(filtered_risky_symbol):

    Fund_Allocation = get_sql(level1_csmar, f'select * from Fund_Allocation_m where EquityProportion > 0.3 and Symbol in {tuple(filtered_risky_symbol)}')
    Fund_Allocation_filter = pd.MultiIndex.from_frame(Fund_Allocation[['Date', 'Symbol']])

    return Fund_Allocation_filter


def get_Fund_Return_m(start_date, end_date):
    Fund_Return_m = get_sql(level2_csmar, 'Fund_Return_m')
    Fund_Return_m['Date'] = pd.to_datetime(Fund_Return_m['Date'])
    Fund_Return_m = Fund_Return_m.assign(Date=lambda df: df.Date.dt.to_period('M'))
    #r下标是t+1，所以要往前推一个月
    Fund_Return_m = Fund_Return_m.loc[(Fund_Return_m['Date'] >= start_date + pd.offsets.MonthEnd(1)) 
                                      & (Fund_Return_m['Date'] <= end_date + pd.offsets.MonthEnd(1))]
    Fund_Return_m.loc[:, 'Date'] = Fund_Return_m['Date'] - pd.offsets.MonthEnd(1)
    Fund_Return_m = Fund_Return_m.set_index(['Symbol', 'Date'])

    return Fund_Return_m


def get_factors(factors, start_date, end_date):

    factors_df = pd.DataFrame()

    for factor, _ in factors:
        temp_df = get_sql(level3_factors, factor)
        temp_df['Date'] = pd.to_datetime(temp_df['Date'])
        temp_df = temp_df.assign(Date=lambda df: df.Date.dt.to_period('M'))
        temp_df = temp_df.loc[(temp_df['Date'] >= start_date) & (temp_df['Date'] <= end_date)]

        if factors_df.empty:
            factors_df = temp_df
        else:
            factors_df = pd.merge(factors_df, temp_df, on=['Symbol', 'Date'], how='outer')

    factors_df = factors_df.set_index(['Symbol', 'Date'])
    return factors_df


def get_total_x_y(start_date, end_date, factors):

    total_x_y = pd.merge(get_Fund_Return_m(start_date, end_date), get_factors(factors, start_date, end_date), on=['Symbol', 'Date'], how='outer')
    return total_x_y


def handle_data(Symbol_filter, time_windows=12, end_date=last_month_begin_s):

    end_date = pd.to_datetime(end_date).to_period('M') 
    start_date = end_date - pd.offsets.MonthEnd(time_windows)
    # start_date = pd.to_datetime('2001-01').to_period('M') 
    '''
    factors = [('Fund_IdioVol', 'high'),  # Trading Friction
                ('Fund_Rel2High', 'high'),
                ('Fund_ResidVar', 'high'),
                ('Fund_AT', 'low'),
                ('Fund_CAPM_beta', 'high'),
                ('Fund_LME', 'low'),
                ('Fund_LTurnover', 'high'),
                ('Fund_MktBeta', 'high'),
                ('Fund_r2_1', 'high'),  # past return
                ('Fund_r12_2', 'high'),
                ('Fund_r12_7', 'high'),
                ('Fund_r36_13', 'high'),
                ('Fund_Investment', 'high'),  # Investment
                ('Fund_NOA', 'high'),
                ('Fund_DPI2A', 'high'),
                ('Fund_NSI', 'high')]
    '''
    factors = [
               ('capm_α', 'high'),  # now factors
               ('svc_α', 'high'),
            #    ('capm_α_ε', 'high'),
            #    ('svc_α_ε', 'high'),
               ('industry_concentration', 'high'),
               ('connected_companies_portfolio_capm', 'high'), 
               ('connected_companies_portfolio_svc', 'high'),
               ('active_share', 'high'),
               ('return_gap', 'high'),
               ('ME', 'low'), # 列名为EsitmatedValue_m
            #    ('TD_α', 'high'), # 只有债券型有
            #    ('TDP_α', 'high'), # 只有债券型有
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
               ('Fund_age', 'low'),  # FundCharacters 列名为Age
               ('Fund_tna', 'low'), # FundCharacters 列名为TNA
               ('F_r12_2', 'high'),  # FundMomentum
               ('F_ST_Rev', 'high')
    ]
    
    total_x_y = get_total_x_y(start_date, end_date, factors)
    # total_x_y = total_x_y.dropna(subset=['Fund_Return_m'])
    total_x_y = total_x_y[total_x_y.index.get_level_values(0).isin(Symbol_filter)]
    # total_x_y = total_x_y.fillna(total_x_y.groupby(level=1).transform('median'))
    for column in  total_x_y.columns[1:]:
        if factors[total_x_y.columns.get_loc(column) - 1][1] == 'high':
            total_x_y[column] = total_x_y.groupby(level=1)[column].transform(lambda x: (x.rank() / len(x)) - 0.5)
        else:
            total_x_y[column] = total_x_y.groupby(level=1)[column].transform(lambda x: (x.rank(ascending=False) / len(x)) - 0.5)
    
    return total_x_y


def predict_with_ipca(data, n_factors=5, iter_tol=0.01, alpha=0.0005,l1_ratio=0.0005,max_iter=100):

    all_ypred = pd.DataFrame()
    regr = InstrumentedPCA(n_factors, intercept=False, iter_tol=iter_tol,alpha=alpha,l1_ratio=l1_ratio,max_iter=max_iter) #无截距项
    data = data.sort_index(level=1) #按时间sort
   
    predict_date = data.index.get_level_values(1).unique()[-1]
    data_IS = data[(data.index.get_level_values("Date") != predict_date)].dropna(subset=['Fund_Return_m'])
    data_OOS = data[(data.index.get_level_values("Date") == predict_date)]
    x_IS = data_IS.drop('Fund_Return_m', axis=1)
    y_IS = data_IS['Fund_Return_m']
    x_OOS = data_OOS.drop('Fund_Return_m', axis=1)
    y_OOS = data_OOS['Fund_Return_m']
    y_IS.index = y_IS.index.set_levels(y_IS.index.levels[1].map(convert_date_to_int), level=1) #用alpha和l1时index要转换成int
    y_OOS.index = y_OOS.index.set_levels(y_OOS.index.levels[1].map(convert_date_to_int), level=1) 
    x_IS.index = x_IS.index.set_levels(x_IS.index.levels[1].map(convert_date_to_int), level=1) 
    x_OOS.index = x_OOS.index.set_levels(x_OOS.index.levels[1].map(convert_date_to_int), level=1) 
    nan_cols_IS = x_IS.columns[x_IS.isna().all()].tolist()
    nan_cols_OOS = x_OOS.columns[x_OOS.isna().all()].tolist()
    nan_cols = list(set(nan_cols_IS + nan_cols_OOS))
    x_IS = x_IS.drop(nan_cols, axis=1)
    x_OOS = x_OOS.drop(nan_cols, axis=1)
    print(f'drop {nan_cols} because they are all nan')
    if x_OOS.empty:
        print(f'{predict_date}x_OOS is empty')
    else:
        regr = regr.fit(X=x_IS, y=y_IS)
        Ypred = regr.predict(X=x_OOS, mean_factor=True)
        index_noNA = x_OOS.dropna().index
        new_data = {
            'Ypred': Ypred.flatten()
        }
        new_row = pd.DataFrame(new_data, index=index_noNA)
        all_ypred = pd.concat([all_ypred, new_row])
         
    data.index = data.index.set_levels(data.index.levels[1].map(convert_date_to_int), level=1) 
    merged_data_ypred = data.merge(all_ypred, left_index=True, right_index=True, how='right')
    merged_data_ypred=merged_data_ypred.reset_index()
    merged_data_ypred['Date']=pd.to_datetime(merged_data_ypred['Date'],format='%Y%m') + pd.offsets.MonthBegin(1)
    # merged_data_ypred=merged_data_ypred.set_index(['Symbol', 'Date'])
    merged_data_ypred = merged_data_ypred[['Symbol', 'Date', 'Ypred']]
    merged_data_ypred['Ypred'] = (merged_data_ypred['Ypred'] - merged_data_ypred['Ypred'].mean()) / merged_data_ypred['Ypred'].std()
    merged_data_ypred.rename(columns={'Ypred': 'ipca_score'}, inplace=True)

    return merged_data_ypred


def upload(merged_data_ypred):
    merged_data_ypred.to_sql('ipca_score', con=level3_factors, if_exists='append', index=False)
    

@timeit('ipca_score')
def ipca_score():
    result = pd.DataFrame()
    try:
        ipca_score = get_sql(level3_factors, 'ipca_score')
        ipca_score = ipca_score.dropna()
        latest_date = ipca_score['Date'].max()
        if pd.isnull(latest_date):
            latest_date = pd.to_datetime('2023-12-01')
            print(f'[+] {time.strftime("%c")} ipca_score is empty')
    except:
        latest_date = pd.to_datetime('2023-12-01')
    if latest_date == pd.to_datetime(this_month_begin):
        print(f'ipca_score is already the latest, {latest_date}')
        pass
    else:
        Classification = read_sql()
        Fund_Allocation_filter = get_Fund_Allocation_filter(get_filtered_risky_symbol(Classification))
        Symbol_filter = Fund_Allocation_filter.get_level_values(1).unique()
        for i in range((pd.to_datetime(this_month_begin).to_period('M') - latest_date.to_period('M')).n-1,-1,-1):
            print(f'[+] {time.strftime("%c")} update ipca_score...', (last_month_begin-pd.offsets.MonthBegin(i-1)).strftime('%Y-%m-%d'))
            data = handle_data(Symbol_filter, end_date=(last_month_begin-pd.offsets.MonthBegin(i)).strftime('%Y-%m-%d'))
            merged_data_ypred = predict_with_ipca(data)
            result = pd.concat([result, merged_data_ypred])

        upload(result)

# if __name__ == '__main__':
#     ipca_score()