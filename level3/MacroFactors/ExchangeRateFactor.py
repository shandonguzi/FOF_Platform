import pandas as pd

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    USDIndex = get_sql(level0_wind, 'USDIndex')
    return USDIndex


def handle(USDIndex):  

    USDIndex = USDIndex.set_index('Date')
    # 同比
    ExchangeRateFactor_YoY = pd.DataFrame()
    ExchangeRateFactor_YoY['ExchangeRateFactor_YoY'] = USDIndex.pct_change(periods=250) * 100
    ExchangeRateFactor_YoY = ExchangeRateFactor_YoY.dropna()
    # 周环比
    ExchangeRateFactor_WoW = pd.DataFrame()
    ExchangeRateFactor_WoW['ExchangeRateFactor_WoW'] = USDIndex.pct_change(periods=5) * 100
    ExchangeRateFactor_WoW = ExchangeRateFactor_WoW.dropna()

    return ExchangeRateFactor_YoY, ExchangeRateFactor_WoW


def upload(ExchangeRateFactor_YoY, ExchangeRateFactor_WoW):

    ExchangeRateFactor_YoY = ExchangeRateFactor_YoY.reset_index()
    ExchangeRateFactor_WoW = ExchangeRateFactor_WoW.reset_index()
    ExchangeRateFactor_YoY.to_sql('ExchangeRateFactor_YoY', con=level3_factors, if_exists='replace', index=False)
    ExchangeRateFactor_WoW.to_sql('ExchangeRateFactor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/ExchangeRateFactor')
def ExchangeRateFactor():
    USDIndex = read_sql()
    ExchangeRateFactor_YoY, ExchangeRateFactor_WoW = handle(USDIndex)
    upload(ExchangeRateFactor_YoY, ExchangeRateFactor_WoW)


if __name__ == '__main__':
    ExchangeRateFactor()

    




