import pandas as pd

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    CB_GovBondIndex = get_sql(level0_wind, 'CB_GovBondIndex')
    return CB_GovBondIndex


def handle(CB_GovBondIndex):  

    CB_GovBondIndex = CB_GovBondIndex.set_index('Date')
    # 同比
    InterestRateFactor_YoY = pd.DataFrame()
    InterestRateFactor_YoY['InterestRateFactor_YoY'] = -CB_GovBondIndex.pct_change(periods=250) * 100
    InterestRateFactor_YoY = InterestRateFactor_YoY.dropna()
    # 周环比
    InterestRateFactor_WoW = pd.DataFrame()
    InterestRateFactor_WoW['InterestRateFactor_WoW'] = -CB_GovBondIndex.pct_change(periods=5) * 100
    InterestRateFactor_WoW = InterestRateFactor_WoW.dropna()
    
    return InterestRateFactor_YoY, InterestRateFactor_WoW


def upload(InterestRateFactor_YoY, InterestRateFactor_WoW):
    
    InterestRateFactor_YoY = InterestRateFactor_YoY.reset_index()
    InterestRateFactor_WoW = InterestRateFactor_WoW.reset_index()
    InterestRateFactor_YoY.to_sql('InterestRateFactor_YoY', con=level3_factors, if_exists='replace', index=False)
    InterestRateFactor_WoW.to_sql('InterestRateFactor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/InterestRateFactor')
def InterestRateFactor():
    CB_GovBondIndex = read_sql()
    InterestRateFactor_YoY, InterestRateFactor_WoW = handle(CB_GovBondIndex)
    upload(InterestRateFactor_YoY, InterestRateFactor_WoW)


if __name__ == '__main__':
    InterestRateFactor()

    




