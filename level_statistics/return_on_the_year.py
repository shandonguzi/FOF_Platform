
import pandas as pd

from settings.database import level1_csmar, level1_wind, errata_jiayin_robo_advisor, statistics_level4
from utils.evaluate.get_yearly_return import get_yearly_return
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def get_return_on_the_year(return_matrix, base):
    on_the_year = get_yearly_return(return_matrix)
    base_return = get_sql(level1_csmar, f'select * from {base}_daily', index_cols='Date')
    base_on_the_year = get_yearly_return(base_return)
    on_the_year = pd.concat([on_the_year, base_on_the_year], axis=1)
    columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '沪深300']
    on_the_year.columns = columns
    on_the_year.index.name = '年份'
    return on_the_year

def get_return_on_the_year_(return_matrix, base):
    on_the_year = get_yearly_return(return_matrix)
    base_return = get_sql(level1_wind, f'select * from {base}', index_cols='Date')
    base_on_the_year = get_yearly_return(base_return)
    on_the_year = pd.concat([on_the_year, base_on_the_year], axis=1)
    columns = ['九支基金', '十支基金', '十五支基金', '三十支基金', '一年期国债收益率']
    on_the_year.columns = columns
    on_the_year.index.name = '年份'
    return on_the_year


def read_sql():
    funds_return_0 = get_sql(errata_jiayin_robo_advisor, 'funds_return_0', index_cols='Date')
    funds_return_1 = get_sql(errata_jiayin_robo_advisor, 'funds_return_1', index_cols='Date')
    funds_return_2 = get_sql(errata_jiayin_robo_advisor, 'funds_return_2', index_cols='Date')
    funds_return_3 = get_sql(errata_jiayin_robo_advisor, 'funds_return_3', index_cols='Date')
    funds_return_4 = get_sql(errata_jiayin_robo_advisor, 'funds_return_4', index_cols='Date')
    funds_return_5 = get_sql(errata_jiayin_robo_advisor, 'funds_return_5', index_cols='Date')
    return funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5


def handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5):
    
    on_the_year_0 = get_return_on_the_year_(funds_return_0, 'TreasuryBond1D')
    on_the_year_1 = get_return_on_the_year_(funds_return_1, 'TreasuryBond1D')
    on_the_year_2 = get_return_on_the_year(funds_return_2, 'hs300')
    on_the_year_3 = get_return_on_the_year(funds_return_3, 'hs300')
    on_the_year_4 = get_return_on_the_year(funds_return_4, 'hs300')
    on_the_year_5 = get_return_on_the_year(funds_return_5, 'hs300')
    on_the_year_all = pd.DataFrame([on_the_year_0['十五支基金'], on_the_year_1['十五支基金'], on_the_year_2['十五支基金'], on_the_year_3['十五支基金'], on_the_year_4['十五支基金'], on_the_year_5['沪深300'], on_the_year_5['十五支基金']],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '沪深300', '极端收益'], name='类别')).T
    on_the_year_all = on_the_year_all[on_the_year_all.index >= 2010]

    return on_the_year_0, on_the_year_1, on_the_year_2, on_the_year_3, on_the_year_4, on_the_year_5, on_the_year_all

def upload(on_the_year_0, on_the_year_1, on_the_year_2, on_the_year_3, on_the_year_4, on_the_year_5, on_the_year_all):
    on_the_year_0.to_sql('on_the_year_0', con=statistics_level4, if_exists='replace')
    on_the_year_1.to_sql('on_the_year_1', con=statistics_level4, if_exists='replace')
    on_the_year_2.to_sql('on_the_year_2', con=statistics_level4, if_exists='replace')
    on_the_year_3.to_sql('on_the_year_3', con=statistics_level4, if_exists='replace')
    on_the_year_4.to_sql('on_the_year_4', con=statistics_level4, if_exists='replace')
    on_the_year_5.to_sql('on_the_year_5', con=statistics_level4, if_exists='replace')
    on_the_year_all.to_sql('on_the_year_all', con=statistics_level4, if_exists='replace')

@timeit('level4/return_on_the_year')
def return_on_the_year():
    funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5 = read_sql()
    on_the_year_0, on_the_year_1, on_the_year_2, on_the_year_3, on_the_year_4, on_the_year_5, on_the_year_all = handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5)
    upload(on_the_year_0, on_the_year_1, on_the_year_2, on_the_year_3, on_the_year_4, on_the_year_5, on_the_year_all)
