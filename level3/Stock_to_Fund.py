from settings.database import *
from utils.mysql.get_sql import get_sql
import numpy as np
import pandas as pd
import time
from utils.time_function import timeit
import dask.dataframe as dd


def expand_to_monthly(group):
    # 为每个季度的起始月份生成三个月的范围
    start_date = group.name
    months = pd.date_range(start=(start_date - pd.offsets.MonthBegin(1)), periods=3, freq='MS')

    return group.loc[np.repeat(group.index.values, 3)].assign(Date=np.tile(months, len(group)))


def remove_outliers_with_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    return df[(df[column] >= (Q1 - 1.5 * IQR)) & (df[column] <= (Q3 + 1.5 * IQR))]


def normalize(df, column, is_high):
    df['normalized_factors'] = df.groupby('Date')[column].transform(
        lambda x: (x.rank(ascending=is_high) - 1) / len(x) - 0.5
    )
    return df[['Date', 'Stkcd', 'normalized_factors']].rename(columns={'normalized_factors': column})


def handle(ddf_Portfolio, factor_name, is_high):
    start_time = time.time()
    print(f"\n[+] Fund_{factor_name} start at {time.strftime('%c')}")

    factor = get_sql(level3_factors, factor_name)
    factor['Date'] = pd.to_datetime(factor['Date'])
    factor_processed = factor.groupby('Date').apply(remove_outliers_with_iqr, factor_name).reset_index(drop=True)
    factor_processed = normalize(factor_processed, factor_name, is_high=is_high)
    factor_processed['Stkcd'] = factor_processed['Stkcd'].astype(np.int32)
    factor_processed[factor_name] = factor_processed[factor_name].astype(np.float16)

    ddf_factor = dd.from_pandas(factor_processed, npartitions=10)
    ddf_merged = dd.merge(ddf_Portfolio, ddf_factor, on=['Date', 'Stkcd'], how='left')

    time.sleep(30)
    df_merged = ddf_merged.compute()

    weighted_factors = df_merged.groupby(['Date', 'Symbol']).apply(lambda x: np.sum(x['Proportion'] * x[factor_name])).reset_index(name=factor_name)
    weighted_factors.to_sql(f'Fund_{factor_name}', level3_factors, if_exists='replace', index=False)

    print(f"[=] Fund_{factor_name} finish at {time.strftime('%c')}")
    print(f"[=] Cost {round(time.time() - start_time, 1)} seconds\n")


@timeit('Stock_to_Fund')
def Stock_to_Fund():
    '''
    月度脚本，待股票脚本生成新一月数据后再跑
    '''

    Fund_Portfolio_Stock = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    Fund_Portfolio_Stock['Date'] = pd.to_datetime(Fund_Portfolio_Stock['Date'])
    Portfolio_monthly = Fund_Portfolio_Stock.groupby('Date').apply(expand_to_monthly).reset_index(drop=True)
    Portfolio_monthly['Symbol'] = Portfolio_monthly['Symbol'].astype(np.int32)
    Portfolio_monthly['Stkcd'] = Portfolio_monthly['Stkcd'].astype(np.int32)
    Portfolio_monthly['Proportion'] = Portfolio_monthly['Proportion'].astype(np.float16)
    Portfolio_monthly.drop('StockName', axis=1, inplace=True)
    ddf_Portfolio = dd.from_pandas(Portfolio_monthly, npartitions=30)

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

    for each_factor, is_high in factors:
        handle(ddf_Portfolio, each_factor, is_high)

