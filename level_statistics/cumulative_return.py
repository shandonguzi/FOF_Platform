
import pandas as pd
import numpy as np

from settings.database import errata_jiayin_robo_advisor, statistics_level4, level1_csmar, level1_wind
from utils.frequent_dates import last_month_end
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    funds_return_0 = get_sql(errata_jiayin_robo_advisor, 'funds_return_0', index_cols='Date')
    funds_return_1 = get_sql(errata_jiayin_robo_advisor, 'funds_return_1', index_cols='Date')
    funds_return_2 = get_sql(errata_jiayin_robo_advisor, 'funds_return_2', index_cols='Date')
    funds_return_3 = get_sql(errata_jiayin_robo_advisor, 'funds_return_3', index_cols='Date')
    funds_return_4 = get_sql(errata_jiayin_robo_advisor, 'funds_return_4', index_cols='Date')
    funds_return_5 = get_sql(errata_jiayin_robo_advisor, 'funds_return_5', index_cols='Date')
    return funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5


def get_cum_ret(funds_return, start_time):
    base = get_sql(level1_csmar, 'hs300_monthly', index_cols='Date').ed2ed
    base.index = base.index - pd.offsets.MonthBegin(1)
    base = base.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = funds_return.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = pd.concat([funds_return, base], axis=1)
    cum_ret = (funds_return.loc[start_time: ] + 1).cumprod()
    cum_ret.index.name = '时间'
    cum_ret.columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '沪深300']
    return cum_ret.loc[: last_month_end]


def get_cum_ret_(funds_return, start_time):
    base = get_sql(level1_wind, 'TreasuryBond1M', index_cols='Date')
    base.index = base.index - pd.offsets.MonthBegin(1)
    base = base.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = funds_return.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = pd.concat([funds_return, base], axis=1)
    cum_ret = (funds_return.loc[start_time: ] + 1).cumprod()
    cum_ret.index.name = '时间'
    cum_ret.columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '一年期国债收益率']
    return cum_ret.loc[: last_month_end]

def handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5):

    start_time = pd.Timestamp('2019-1')
    cum_ret_0 = get_cum_ret_(funds_return_0, start_time)
    cum_ret_1 = get_cum_ret_(funds_return_1, start_time)
    cum_ret_2 = get_cum_ret(funds_return_2, start_time)
    cum_ret_3 = get_cum_ret(funds_return_3, start_time)
    cum_ret_4 = get_cum_ret(funds_return_4, start_time)
    cum_ret_5 = get_cum_ret(funds_return_5, start_time)
    
    cum_ret_all = pd.DataFrame([cum_ret_0['十五支基金'], cum_ret_1['十五支基金'], cum_ret_2['十五支基金'], cum_ret_3['十五支基金'], cum_ret_4['十五支基金'], cum_ret_5['十五支基金']],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '极端收益'], name='类别')).T
    
    return cum_ret_0, cum_ret_1, cum_ret_2, cum_ret_3, cum_ret_4, cum_ret_5, cum_ret_all

def upload(cum_ret_0, cum_ret_1, cum_ret_2, cum_ret_3, cum_ret_4, cum_ret_5, cum_ret_all):
    cum_ret_0.to_sql('cum_ret_0', con=statistics_level4, if_exists='replace')
    cum_ret_1.to_sql('cum_ret_1', con=statistics_level4, if_exists='replace')
    cum_ret_2.to_sql('cum_ret_2', con=statistics_level4, if_exists='replace')
    cum_ret_3.to_sql('cum_ret_3', con=statistics_level4, if_exists='replace')
    cum_ret_4.to_sql('cum_ret_4', con=statistics_level4, if_exists='replace')
    cum_ret_5.to_sql('cum_ret_5', con=statistics_level4, if_exists='replace')
    cum_ret_all.to_sql('cum_ret_all', con=statistics_level4, if_exists='replace')

def handle_all(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5):

    start_time = pd.Timestamp('2010-1')
    cum_ret_0_all = get_cum_ret_(funds_return_0, start_time)
    cum_ret_1_all = get_cum_ret_(funds_return_1, start_time)
    cum_ret_2_all = get_cum_ret(funds_return_2, start_time)
    cum_ret_3_all = get_cum_ret(funds_return_3, start_time)
    cum_ret_4_all = get_cum_ret(funds_return_4, start_time)
    cum_ret_5_all = get_cum_ret(funds_return_5, start_time)

    cum_ret_all_all = pd.DataFrame([cum_ret_0_all['十五支基金'], cum_ret_1_all['十五支基金'], cum_ret_2_all['十五支基金'], cum_ret_3_all['十五支基金'], cum_ret_4_all['十五支基金'], cum_ret_5_all['十五支基金']],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '极端收益'], name='类别')).T
    
    return cum_ret_0_all, cum_ret_1_all, cum_ret_2_all, cum_ret_3_all, cum_ret_4_all, cum_ret_5_all, cum_ret_all_all

def upload_all(cum_ret_0_all, cum_ret_1_all, cum_ret_2_all, cum_ret_3_all, cum_ret_4_all, cum_ret_5_all, cum_ret_all_all):
    cum_ret_0_all.to_sql('cum_ret_0_all', con=statistics_level4, if_exists='replace')
    cum_ret_1_all.to_sql('cum_ret_1_all', con=statistics_level4, if_exists='replace')
    cum_ret_2_all.to_sql('cum_ret_2_all', con=statistics_level4, if_exists='replace')
    cum_ret_3_all.to_sql('cum_ret_3_all', con=statistics_level4, if_exists='replace')
    cum_ret_4_all.to_sql('cum_ret_4_all', con=statistics_level4, if_exists='replace')
    cum_ret_5_all.to_sql('cum_ret_5_all', con=statistics_level4, if_exists='replace')
    cum_ret_all_all.to_sql('cum_ret_all_all', con=statistics_level4, if_exists='replace')

@timeit('level4/cumulative_return')
def cumulative_return():
    funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5 = read_sql()
    cum_ret_0, cum_ret_1, cum_ret_2, cum_ret_3, cum_ret_4, cum_ret_5, cum_ret_all = handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5)
    upload(cum_ret_0, cum_ret_1, cum_ret_2, cum_ret_3, cum_ret_4, cum_ret_5, cum_ret_all)

    cum_ret_0_all, cum_ret_1_all, cum_ret_2_all, cum_ret_3_all, cum_ret_4_all, cum_ret_5_all, cum_ret_all_all = handle_all(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5)
    upload_all(cum_ret_0_all, cum_ret_1_all, cum_ret_2_all, cum_ret_3_all, cum_ret_4_all, cum_ret_5_all, cum_ret_all_all)
