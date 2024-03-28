
import numpy as np
import pandas as pd
from settings.database import statistics_level4, level1_csmar, level1_wind, errata_jiayin_robo_advisor

from sqlalchemy import VARCHAR
from utils.evaluate.get_annual_sharpe_return_std_β import get_annual_return, get_annual_sharpe, get_annual_volatility, get_β, get_past_year_mdd, get_mdd
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
import statsmodels.api as sm

def read_sql():
    funds_return_0 = get_sql(errata_jiayin_robo_advisor, 'funds_return_0', index_cols='Date')
    funds_return_1 = get_sql(errata_jiayin_robo_advisor, 'funds_return_1', index_cols='Date')
    funds_return_2 = get_sql(errata_jiayin_robo_advisor, 'funds_return_2', index_cols='Date')
    funds_return_3 = get_sql(errata_jiayin_robo_advisor, 'funds_return_3', index_cols='Date')
    funds_return_4 = get_sql(errata_jiayin_robo_advisor, 'funds_return_4', index_cols='Date')
    funds_return_5 = get_sql(errata_jiayin_robo_advisor, 'funds_return_5', index_cols='Date')
    return funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5

def get_β(portfolio_return):
    portfolio_return = portfolio_return.dropna()
    β = portfolio_return.apply(lambda y: sm.OLS(y, portfolio_return.ed2ed).fit().params[0])
    β.name = 'β'
    return β

def get_funds_statistics(funds_return):
    mdd_month = 3
    funds_return = funds_return.loc['2010-1': ]
    annual_sharpe = get_annual_sharpe(funds_return)
    annual_return = get_annual_return(funds_return)
    annual_volatility = get_annual_volatility(funds_return)
    β = get_β(funds_return)
    past_year_mdd = get_past_year_mdd(funds_return, mdd_month)
    mdd = get_mdd(funds_return, mdd_month)
    advanced_statistics = pd.concat([annual_sharpe, annual_volatility, annual_return, β, past_year_mdd, mdd], axis=1)
    return advanced_statistics


def _funds_statistics(funds_return):
    base = get_sql(level1_csmar, 'hs300_monthly', index_cols='Date').ed2ed
    base.index = base.index - pd.offsets.MonthBegin(1)
    base = base.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = funds_return.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = pd.concat([funds_return, base], axis=1)
    advanced_statistics = get_funds_statistics(funds_return)
    advanced_statistics.index = ['九支基金', '十支基金', '十五支基金', '三十支基金', '沪深300']
    advanced_statistics.index.name = '类别'
    advanced_statistics.columns = ['年化夏普比率', '年化波动率', '年化收益率', '贝塔', '过去一年三个月最大回撤', '成立以来三个月最大回撤']
    return advanced_statistics

def __funds_statistics(funds_return):
    base = get_sql(level1_wind, 'TreasuryBond1M', index_cols='Date')
    base.index = base.index - pd.offsets.MonthBegin(1)
    base = base.loc[np.intersect1d(base.index, funds_return.index)]
    base.name = 'ed2ed'
    funds_return = funds_return.loc[np.intersect1d(base.index, funds_return.index)]
    funds_return = pd.concat([funds_return, base], axis=1)
    advanced_statistics = get_funds_statistics(funds_return)
    advanced_statistics.index = ['九支基金', '十支基金', '十五支基金', '三十支基金', '一年期国债收益率']
    advanced_statistics.index.name = '类别'
    advanced_statistics.columns = ['年化夏普比率', '年化波动率', '年化收益率', '贝塔', '过去一年三个月最大回撤', '成立以来三个月最大回撤']
    return advanced_statistics

def handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5):

    funds_statistics_0 = __funds_statistics(funds_return_0)
    funds_statistics_1 = __funds_statistics(funds_return_1)
    funds_statistics_2 = _funds_statistics(funds_return_2)
    funds_statistics_3 = _funds_statistics(funds_return_3)
    funds_statistics_4 = _funds_statistics(funds_return_4)
    funds_statistics_5 = _funds_statistics(funds_return_5)

    funds_statistics_all = pd.DataFrame([funds_statistics_0.loc['十五支基金'], funds_statistics_1.loc['十五支基金'], \
                                        funds_statistics_2.loc['十五支基金'], funds_statistics_3.loc['十五支基金'], \
                                        funds_statistics_4.loc['十五支基金'], funds_statistics_4.loc['沪深300'], funds_statistics_5.loc['十五支基金']],\
                  index=pd.Series(['活钱管理', '稳健理财', '跑赢通胀', '追求增值', '追求高收益', '沪深300', '极端收益'], name='类别'))

    return funds_statistics_0, funds_statistics_1, funds_statistics_2, funds_statistics_3, funds_statistics_4, funds_statistics_5, funds_statistics_all

def upload(funds_statistics_0, funds_statistics_1, funds_statistics_2, funds_statistics_3, funds_statistics_4, funds_statistics_5, funds_statistics_all):

    funds_statistics_0.to_sql('funds_statistics_0', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(10)})
    funds_statistics_1.to_sql('funds_statistics_1', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})
    funds_statistics_2.to_sql('funds_statistics_2', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})
    funds_statistics_3.to_sql('funds_statistics_3', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})
    funds_statistics_4.to_sql('funds_statistics_4', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})
    funds_statistics_5.to_sql('funds_statistics_5', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})
    funds_statistics_all.to_sql('funds_statistics_all', con=statistics_level4, if_exists='replace', dtype={'类别': VARCHAR(30)})

@timeit('level4/advanced_statistics')
def advanced_statistics():
    funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5 = read_sql()
    funds_statistics_0, funds_statistics_1, funds_statistics_2, funds_statistics_3, funds_statistics_4, funds_statistics_5, funds_statistics_all = handle(funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4, funds_return_5)
    upload(funds_statistics_0, funds_statistics_1, funds_statistics_2, funds_statistics_3, funds_statistics_4, funds_statistics_5, funds_statistics_all)
