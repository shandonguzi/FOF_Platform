import pandas as pd

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    CB_MidShortNoteAAAYTM5Y = get_sql(level0_wind, 'CB_MidShortNoteAAAYTM5Y')
    CB_GovBondYTM5Y = get_sql(level0_wind, 'CB_GovBondYTM5Y')
    return CB_MidShortNoteAAAYTM5Y, CB_GovBondYTM5Y



def handle(CB_MidShortNoteAAAYTM5Y, CB_GovBondYTM5Y):  

    CB_MidShortNoteAAAYTM5Y = CB_MidShortNoteAAAYTM5Y.set_index('Date')
    CB_GovBondYTM5Y = CB_GovBondYTM5Y.set_index('Date')
    CreditSpread = pd.merge(CB_MidShortNoteAAAYTM5Y, CB_GovBondYTM5Y, on='Date', how='inner')
    CreditSpread['CreditSpread'] = CreditSpread['CB_MidShortNoteAAAYTM5Y'] - CreditSpread['CB_GovBondYTM5Y']
    CreditSpread = CreditSpread.dropna()
    # 同比
    CreditSpreadFactor_YoY = pd.DataFrame()
    CreditSpreadFactor_YoY['CreditSpreadFactor_YoY'] = CreditSpread['CreditSpread'].pct_change(periods=250) 
    CreditSpreadFactor_YoY = CreditSpreadFactor_YoY.dropna()
    # 周环比
    CreditSpreadFactor_WoW = pd.DataFrame()
    CreditSpreadFactor_WoW['CreditSpreadFactor_WoW'] = CreditSpread['CreditSpread'].pct_change(periods=5) 
    CreditSpreadFactor_WoW = CreditSpreadFactor_WoW.dropna()

    return CreditSpreadFactor_YoY, CreditSpreadFactor_WoW


def upload(CreditSpreadFactor_YoY, CreditSpreadFactor_WoW):

    CreditSpreadFactor_YoY = CreditSpreadFactor_YoY.reset_index()
    CreditSpreadFactor_WoW = CreditSpreadFactor_WoW.reset_index()
    CreditSpreadFactor_YoY.to_sql('CreditSpreadFactor_YoY', con=level3_factors, if_exists='replace', index=False)
    CreditSpreadFactor_WoW.to_sql('CreditSpreadFactor_WoW', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/CreditSpreadFactor')
def CreditSpreadFactor():
    CB_MidShortNoteAAAYTM5Y, CB_GovBondYTM5Y = read_sql()
    CreditSpreadFactor_YoY, CreditSpreadFactor_WoW = handle(CB_MidShortNoteAAAYTM5Y, CB_GovBondYTM5Y)
    upload(CreditSpreadFactor_YoY, CreditSpreadFactor_WoW)


if __name__ == '__main__':
    CreditSpreadFactor()

    




