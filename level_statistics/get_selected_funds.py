
import numpy as np
import pandas as pd
from settings.database import errata_jiayin_robo_advisor, statistics_level4

from sqlalchemy import VARCHAR
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from utils.frequent_dates import this_month_begin_s

def read_sql():

    top_funds_code = get_sql(errata_jiayin_robo_advisor, 'top_funds', index_cols='Date')
    top_funds_name = get_sql(errata_jiayin_robo_advisor, 'top_funds_name', index_cols='Date')

    total_9_funds_weight_0 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_0', index_cols='Date')
    total_9_funds_weight_1 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_1', index_cols='Date')
    total_9_funds_weight_2 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_2', index_cols='Date')
    total_9_funds_weight_3 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_3', index_cols='Date')
    total_9_funds_weight_4 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_4', index_cols='Date')
    total_9_funds_weight_5 = get_sql(errata_jiayin_robo_advisor, 'total_9_funds_weight_5', index_cols='Date')

    total_10_funds_weight_0 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_0', index_cols='Date')
    total_10_funds_weight_1 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_1', index_cols='Date')
    total_10_funds_weight_2 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_2', index_cols='Date')
    total_10_funds_weight_3 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_3', index_cols='Date')
    total_10_funds_weight_4 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_4', index_cols='Date')
    total_10_funds_weight_5 = get_sql(errata_jiayin_robo_advisor, 'total_10_funds_weight_5', index_cols='Date')

    total_15_funds_weight_0 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_0', index_cols='Date')
    total_15_funds_weight_1 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_1', index_cols='Date')
    total_15_funds_weight_2 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_2', index_cols='Date')
    total_15_funds_weight_3 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_3', index_cols='Date')
    total_15_funds_weight_4 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_4', index_cols='Date')
    total_15_funds_weight_5 = get_sql(errata_jiayin_robo_advisor, 'total_15_funds_weight_5', index_cols='Date')

    total_30_funds_weight_0 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_0', index_cols='Date')
    total_30_funds_weight_1 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_1', index_cols='Date')
    total_30_funds_weight_2 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_2', index_cols='Date')
    total_30_funds_weight_3 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_3', index_cols='Date')
    total_30_funds_weight_4 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_4', index_cols='Date')
    total_30_funds_weight_5 = get_sql(errata_jiayin_robo_advisor, 'total_30_funds_weight_5', index_cols='Date')

    return top_funds_code, top_funds_name, \
    [total_9_funds_weight_0, total_10_funds_weight_0, total_15_funds_weight_0, total_30_funds_weight_0], \
    [total_9_funds_weight_1, total_10_funds_weight_1, total_15_funds_weight_1, total_30_funds_weight_1], \
    [total_9_funds_weight_2, total_10_funds_weight_2, total_15_funds_weight_2, total_30_funds_weight_2], \
    [total_9_funds_weight_3, total_10_funds_weight_3, total_15_funds_weight_3, total_30_funds_weight_3], \
    [total_9_funds_weight_4, total_10_funds_weight_4, total_15_funds_weight_4, total_30_funds_weight_4], \
    [total_9_funds_weight_5, total_10_funds_weight_5, total_15_funds_weight_5, total_30_funds_weight_5]


def get_funds(top_funds_code, top_funds_name, top_funds_weight):
    codes = top_funds_code.loc[this_month_begin_s]
    # codes = codes.apply(lambda x: int(x.split('.')[0]))

    names = top_funds_name.loc[this_month_begin_s]

    weights_9 = top_funds_weight[0].loc[this_month_begin_s]
    weights_10 = top_funds_weight[1].loc[this_month_begin_s]
    weights_15 = top_funds_weight[2].loc[this_month_begin_s]
    weights_30 = top_funds_weight[3].loc[this_month_begin_s]

    new_funds = pd.concat([weights_9, weights_10, weights_15, weights_30], axis=1).loc[codes.index]

    new_funds.index = pd.MultiIndex.from_arrays([names.values, codes], names=['基金名称', '基金代码'])
    new_funds.columns = ['九支基金', '十支基金', '十五支基金', '三十支基金']
    new_funds = new_funds.replace(0, np.nan)
    new_funds = new_funds.dropna(how='all').fillna(0)

    return new_funds

def handle(top_funds_code, top_funds_name, top_funds_weight_0, top_funds_weight_1, top_funds_weight_2, top_funds_weight_3, top_funds_weight_4, top_funds_weight_5):

    funds_0 = get_funds(top_funds_code, top_funds_name, top_funds_weight_0)
    funds_1 = get_funds(top_funds_code, top_funds_name, top_funds_weight_1)
    funds_2 = get_funds(top_funds_code, top_funds_name, top_funds_weight_2)
    funds_3 = get_funds(top_funds_code, top_funds_name, top_funds_weight_3)
    funds_4 = get_funds(top_funds_code, top_funds_name, top_funds_weight_4)
    funds_5 = get_funds(top_funds_code, top_funds_name, top_funds_weight_5)

    # funds_0.index = funds_0.index.set_levels(funds_0.index.levels[1].str.zfill(6) + '.OF', level=1)
    # funds_1.index = funds_1.index.set_levels(funds_1.index.levels[1].str.zfill(6) + '.OF', level=1)
    # funds_2.index = funds_2.index.set_levels(funds_2.index.levels[1].str.zfill(6) + '.OF', level=1)
    # funds_3.index = funds_3.index.set_levels(funds_3.index.levels[1].str.zfill(6) + '.OF', level=1)
    # funds_4.index = funds_4.index.set_levels(funds_4.index.levels[1].str.zfill(6) + '.OF', level=1)
    # funds_5.index = funds_5.index.set_levels(funds_5.index.levels[1].str.zfill(6) + '.OF', level=1)

    all_0 = funds_0['十五支基金'].replace(0, np.nan).dropna()
    all_1 = funds_1['十五支基金'].replace(0, np.nan).dropna()
    all_2 = funds_2['十五支基金'].replace(0, np.nan).dropna()
    all_3 = funds_3['十五支基金'].replace(0, np.nan).dropna()
    all_4 = funds_4['十五支基金'].replace(0, np.nan).dropna()
    all_5 = funds_5['十五支基金'].replace(0, np.nan).dropna()


    funds_all = pd.DataFrame([all_0, all_1, all_2, all_3, all_4, all_5],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '极端收益'], name='类别')).T.reset_index().T
    funds_all = funds_all.replace(0, np.nan)
    funds_all = funds_all.dropna(how='all').fillna(0)

    return funds_0, funds_1, funds_2, funds_3, funds_4, funds_5, funds_all

def upload(funds_0, funds_1, funds_2, funds_3, funds_4, funds_5, funds_all):

    funds_0.to_sql('funds_0', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_1.to_sql('funds_1', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_2.to_sql('funds_2', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_3.to_sql('funds_3', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_4.to_sql('funds_4', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_5.to_sql('funds_5', con=statistics_level4, if_exists='replace', dtype={'基金名称': VARCHAR(30), '基金代码': VARCHAR(15)})
    funds_all.to_sql('funds_all', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(5)})

@timeit('level4/get_selected_funds')
def get_selected_funds():
    top_funds_code, top_funds_name, top_funds_weight_0, top_funds_weight_1, top_funds_weight_2, top_funds_weight_3, top_funds_weight_4, top_funds_weight_5 = read_sql()
    funds_0, funds_1, funds_2, funds_3, funds_4, funds_5, funds_all = handle(top_funds_code, top_funds_name, top_funds_weight_0, top_funds_weight_1, top_funds_weight_2, top_funds_weight_3, top_funds_weight_4, top_funds_weight_5)
    upload(funds_0, funds_1, funds_2, funds_3, funds_4, funds_5, funds_all)
