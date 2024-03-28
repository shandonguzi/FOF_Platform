import pandas as pd

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    CB_CorpBondAAAIdx = get_sql(level0_wind, 'CB_CorpBondAAAIdx')
    CB_GovBondOvalIdx = get_sql(level0_wind, 'CB_GovBondOvalIdx')
    return CB_CorpBondAAAIdx, CB_GovBondOvalIdx


def handle(CB_CorpBondAAAIdx, CB_GovBondOvalIdx):  

    duration_Corp = 3  # 假定企业AAA指数的平均久期为3年 最好动态调整，去中债官网上看看
    duration_Gov = 6  # 假定国债指数的平均久期为6年
    x = duration_Gov / (duration_Corp + duration_Gov)
    CB_CorpBondAAAIdx = CB_CorpBondAAAIdx.set_index('Date')
    CB_GovBondOvalIdx = CB_GovBondOvalIdx.set_index('Date')
    CreditFactor = pd.DataFrame()
    CreditFactor['Corp_YoY'] = CB_CorpBondAAAIdx.pct_change(periods=250) * 100
    CreditFactor['Corp_WoW'] = CB_CorpBondAAAIdx.pct_change(periods=5) * 100
    CreditFactor['Gov_YoY'] = CB_GovBondOvalIdx.pct_change(periods=250) * 100
    CreditFactor['Gov_WoW'] = CB_GovBondOvalIdx.pct_change(periods=5) * 100
    # 同比
    CreditFactor_YoY = pd.DataFrame()
    CreditFactor_YoY['CreditFactor_YoY'] = (CreditFactor['Corp_YoY'] * x - CreditFactor['Gov_YoY'] * (1 - x)) * 2
    CreditFactor_YoY = CreditFactor_YoY.dropna()
    # 周环比
    CreditFactor_WoW = pd.DataFrame()
    CreditFactor_WoW['CreditFactor_WoW'] = (CreditFactor['Corp_WoW'] * x - CreditFactor['Gov_WoW'] * (1 - x)) * 2
    CreditFactor_WoW = CreditFactor_WoW.dropna()

    return CreditFactor_YoY, CreditFactor_WoW


def upload(CreditFactor_YoY, CreditFactor_WoW):

    CreditFactor_YoY = CreditFactor_YoY.reset_index()
    CreditFactor_WoW = CreditFactor_WoW.reset_index()
    CreditFactor_YoY.to_sql('CreditFactor_YoY', con=level3_factors, if_exists='replace', index=False)
    CreditFactor_WoW.to_sql('CreditFactor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CreditFactor')
def CreditFactor():
    CB_CorpBondAAAIdx, CB_GovBondOvalIdx = read_sql()
    CreditFactor_YoY, CreditFactor_WoW = handle(CB_CorpBondAAAIdx, CB_GovBondOvalIdx)
    upload(CreditFactor_YoY, CreditFactor_WoW)


if __name__ == '__main__':
    CreditFactor()

    




