
import pandas as pd
from sqlalchemy import VARCHAR

from settings.database import level1_csmar, level1_wind, errata_jiayin_robo_advisor, statistics_level4
from utils.evaluate.get_return_overview import get_overview
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


def _get_overview(return_matrix, base):
    columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '沪深300']
    overview = get_overview(return_matrix, base)
    base_return = get_sql(level1_csmar, f'select * from {base}_daily', index_cols='Date')
    base_overview = get_overview(base_return, base)
    overview[base] = base_overview
    overview.columns = columns
    return overview

def __get_overview(return_matrix, base):
    columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '一年期国债收益率']
    overview = get_overview(return_matrix, base)
    base_return = get_sql(level1_wind, f'select * from {base}', index_cols='Date')
    base_overview = get_overview(base_return, base)
    overview[base] = base_overview
    overview.columns = columns
    return overview

def handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5):

    overview_0 = __get_overview(funds_return_0, 'TreasuryBond1D')
    overview_1 = __get_overview(funds_return_1, 'TreasuryBond1D')
    overview_2 = _get_overview(funds_return_2, 'hs300')
    overview_3 = _get_overview(funds_return_3, 'hs300')
    overview_4 = _get_overview(funds_return_4, 'hs300')
    overview_5 = _get_overview(funds_return_5, 'hs300')
    overview_all = pd.DataFrame([overview_0['十五支基金'], overview_1['十五支基金'], overview_2['十五支基金'], overview_3['十五支基金'], overview_4['十五支基金'], overview_5['沪深300'], overview_5['十五支基金']],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '沪深300', '极端收益'], name='类别')).T
    return overview_0, overview_1, overview_2, overview_3, overview_4, overview_5, overview_all

def upload(overview_0, overview_1, overview_2, overview_3, overview_4, overview_5, overview_all):
    overview_0.to_sql('return_overview_0', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_1.to_sql('return_overview_1', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_2.to_sql('return_overview_2', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_3.to_sql('return_overview_3', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_4.to_sql('return_overview_4', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_5.to_sql('return_overview_5', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})
    overview_all.to_sql('return_overview_all', con=statistics_level4, if_exists='replace', dtype={'过去N月': VARCHAR(15), '统计量': VARCHAR(15)})

@timeit('level4/return_overview')
def return_overview():
    funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5 = read_sql()
    overview_0, overview_1, overview_2, overview_3, overview_4, overview_5, overview_all = handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5)
    upload(overview_0, overview_1, overview_2, overview_3, overview_4, overview_5, overview_all)
