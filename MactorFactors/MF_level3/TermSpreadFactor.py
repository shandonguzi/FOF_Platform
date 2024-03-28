import pandas as pd

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    CB_MidShortBondIndex = get_sql(level0_wind, 'CB_MidShortBondIndex')
    CB_LongBondIndex = get_sql(level0_wind, 'CB_LongBondIndex')
    return CB_MidShortBondIndex, CB_LongBondIndex


def handle(CB_MidShortBondIndex, CB_LongBondIndex):  

    duration_MidShort = 4  # 假定中短期债券的平均久期为4年
    duration_Long = 15  # 假定长期债券的平均久期为15年
    x = duration_Long / (duration_MidShort + duration_Long)
    CB_MidShortBondIndex = CB_MidShortBondIndex.set_index('Date')
    CB_LongBondIndex = CB_LongBondIndex.set_index('Date')
    TermSpreadFactor = pd.DataFrame()
    TermSpreadFactor['MidShort_YoY'] = CB_MidShortBondIndex.pct_change(periods=250) * 100
    TermSpreadFactor['MidShort_WoW'] = CB_MidShortBondIndex.pct_change(periods=5) * 100
    TermSpreadFactor['Long_YoY'] = CB_LongBondIndex.pct_change(periods=250) * 100
    TermSpreadFactor['Long_WoW'] = CB_LongBondIndex.pct_change(periods=5) * 100
    # 同比
    TermSpreadFactor_YoY = pd.DataFrame()
    TermSpreadFactor_YoY['TermSpreadFactor_YoY'] = (TermSpreadFactor['MidShort_YoY'] * x - TermSpreadFactor['Long_YoY'] * (1 - x)) *2
    TermSpreadFactor_YoY = TermSpreadFactor_YoY.dropna()
    # 周环比
    TermSpreadFactor_WoW = pd.DataFrame()
    TermSpreadFactor_WoW['TermSpreadFactor_WoW'] = (TermSpreadFactor['MidShort_WoW'] * x - TermSpreadFactor['Long_WoW'] * (1 - x)) *2
    TermSpreadFactor_WoW = TermSpreadFactor_WoW.dropna()

    return TermSpreadFactor_YoY, TermSpreadFactor_WoW


def upload(TermSpreadFactor_YoY, TermSpreadFactor_WoW):

    TermSpreadFactor_YoY = TermSpreadFactor_YoY.reset_index()
    TermSpreadFactor_WoW = TermSpreadFactor_WoW.reset_index()
    TermSpreadFactor_YoY.to_sql('TermSpreadFactor_YoY', con=level3_factors, if_exists='replace', index=False)
    TermSpreadFactor_WoW.to_sql('TermSpreadFactor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/TermSpreadFactor')
def TermSpreadFactor():
    CB_MidShortBondIndex, CB_LongBondIndex = read_sql()
    TermSpreadFactor_YoY, TermSpreadFactor_WoW = handle(CB_MidShortBondIndex, CB_LongBondIndex)
    upload(TermSpreadFactor_YoY, TermSpreadFactor_WoW)


if __name__ == '__main__':
    TermSpreadFactor()

    




