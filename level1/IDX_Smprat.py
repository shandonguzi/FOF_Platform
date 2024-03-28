
import time

import pandas as pd
import numpy as np

import utils.mysql.p_to_sql
from settings.database import level0_csmar, level1_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    sz50 = get_sql(level0_csmar, "select * from IDX_Smprat where Indexcd = '000016'")
    hs300 = get_sql(level0_csmar, "select * from IDX_Smprat where Indexcd = '000300'")
    zz500 = get_sql(level0_csmar, "select * from IDX_Smprat where Indexcd = '000905'")
    source = [sz50, hs300, zz500]
    try:
        sz50_past = get_sql(level1_csmar, 'sz50_component')
        hs300_past = get_sql(level1_csmar, 'hs300_component')
        zz500_past = get_sql(level1_csmar, 'zz500_component')
        past = [sz50_past, hs300_past, zz500_past]
        return source, past
    except:
        return source, [np.nan, np.nan, np.nan]

def handle(source, past):
    '''handle df'''
    sz50, hs300, zz500 = source[0], source[1], source[2]
    sz50_past, hs300_past, zz500_past = past[0], past[1], past[2]

    sz50['Enddt'] = pd.to_datetime(sz50.Enddt)
    hs300['Enddt'] = pd.to_datetime(hs300.Enddt)
    zz500['Enddt'] = pd.to_datetime(zz500.Enddt)

    sz50 = sz50.rename(columns={'Enddt': 'Date', 'Constdnme': 'Name'})
    hs300 = hs300.rename(columns={'Enddt': 'Date', 'Constdnme': 'Name'})
    zz500 = zz500.rename(columns={'Enddt': 'Date', 'Constdnme': 'Name'})

    sz50 = sz50[['Date', 'Stkcd', 'Weight', 'Name']]
    hs300 = hs300[['Date', 'Stkcd', 'Weight', 'Name']]
    zz500 = zz500[['Date', 'Stkcd', 'Weight', 'Name']]

    if type(sz50_past) != float:
        sz50 = pd.concat([sz50, sz50_past]).drop_duplicates(keep=False)

    if type(hs300_past) != float:
        hs300 = pd.concat([hs300, hs300_past]).drop_duplicates(keep=False)

    if type(zz500_past) != float:
        zz500 = pd.concat([zz500, zz500_past]).drop_duplicates(keep=False)

    return sz50.set_index(['Date', 'Stkcd']), hs300.set_index(['Date', 'Stkcd']), zz500.set_index(['Date', 'Stkcd'])

def upload(sz50, hs300, zz500, past):

    if type(past[0]) == float:
        print(f'[+] {time.strftime("%c")} init 指数成分股权重/sz50')
        sz50.reset_index().p_to_sql('sz50_component', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(sz50) != 0:
            sz50.to_sql('sz50_component', con=level1_csmar, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

    if type(past[1]) == float:
        print(f'[+] {time.strftime("%c")} init 指数成分股权重/hs300')
        hs300.reset_index().p_to_sql('hs300_component', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(hs300) != 0:
            hs300.to_sql('hs300_component', con=level1_csmar, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

    if type(past[2]) == float:
        print(f'[+] {time.strftime("%c")} init 指数成分股权重/zz500')
        zz500.reset_index().p_to_sql('zz500_component', level1_csmar, partitions=1000, n_workers=16, threads_per_worker=1, index=['Date', 'Stkcd'])
    else:
        if len(zz500) != 0:
            zz500.to_sql('zz500_component', con=level1_csmar, if_exists='append')
        else:
            print(f'[×] {time.strftime("%c")} No Data')

@timeit('level1/指数成分股权重/sz50, hs300, zz500')
def IDX_Smprat():
    source, past = read_sql()
    sz50, hs300, zz500 = handle(source, past)
    upload(sz50, hs300, zz500, past)
