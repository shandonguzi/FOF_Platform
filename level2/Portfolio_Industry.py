
import pandas as pd
from parallel_pandas import ParallelPandas
from sqlalchemy import VARCHAR

from settings.database import level0_joinquant, level1_csmar, level2_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

ParallelPandas.initialize(8)

def read_sql():
    csmar_portfolio = get_sql(level1_csmar, 'Fund_Portfolio_Stock')
    hs300_portfolio = get_sql(level1_csmar, 'hs300_component')
    jq_classification = get_sql(level0_joinquant, 'Stock_Classification')
    return csmar_portfolio, hs300_portfolio, jq_classification

def handle(csmar_portfolio, hs300_portfolio, jq_classification):
    csmar_portfolio = pd.merge(csmar_portfolio, jq_classification, on='Stkcd').set_index(['Date', 'Symbol', 'Indnme'])
    hs300_portfolio = pd.merge(hs300_portfolio, jq_classification, on='Stkcd').set_index(['Date', 'Stkcd'])
    hs300_portfolio = hs300_portfolio.groupby(level=1).apply(lambda x: x.droplevel(1).resample('Q').last().ffill()).swaplevel()
    hs300_portfolio = hs300_portfolio.reset_index().groupby(['Date', 'Indnme']).apply(lambda x: x.Weight.sum())
    hs300_portfolio.name = 'Proportion'
    csmar_portfolio = csmar_portfolio[csmar_portfolio.index.get_level_values(0).isin(hs300_portfolio.index.levels[0])]
    csmar_portfolio = csmar_portfolio.groupby(level=0).p_apply(lambda quarter_portfolio: quarter_portfolio.droplevel(0).groupby(level=[0, 1]).apply(lambda x: x.Proportion.sum()))
    csmar_portfolio.name = 'Proportion'
    return hs300_portfolio, csmar_portfolio

def upload(hs300_portfolio, csmar_portfolio):
    hs300_portfolio.to_sql('hs300_portfolio_industry', con=level2_csmar, dtype={'Indnme': VARCHAR(length=12)}, if_exists='replace')
    csmar_portfolio.to_sql('csmar_portfolio_industry', con=level2_csmar, dtype={'Indnme': VARCHAR(length=12)}, if_exists='replace')

@timeit('level2/Portfolio_Industry')
def Portfolio_Industry():
    csmar_portfolio, hs300_portfolio, jq_classification = read_sql()
    hs300_portfolio, csmar_portfolio = handle(csmar_portfolio, hs300_portfolio, jq_classification)
    upload(hs300_portfolio, csmar_portfolio)
