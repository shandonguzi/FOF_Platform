
import pandas as pd
import numpy as np
from utils.frequent_dates import *
from settings.database import level1_csmar, level2_csmar, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit
from functools import reduce


def read_sql():

    data_bs = get_sql(level1_csmar, 'FS_Combas')
    data_cfi = get_sql(level1_csmar, 'FS_Comscfi')
    Stock_last_day = get_sql(level2_csmar, 'Stock_last_day')
    
    return data_bs, data_cfi, Stock_last_day


def handle(data_bs, data_cfi, Stock_last_day):

    finan_rept = reduce(lambda left, right: pd.merge(left, right, on=['Stkcd', 'Date', 'ReportType'], how='outer'), [data_bs, data_cfi])
    finan_rept = finan_rept[finan_rept['Date'].dt.month == 12].reset_index(drop=True)
    finan_rept = finan_rept[finan_rept['ReportType'] == 'A'][['Stkcd', 'Date', 'TotalAssets', 'TotalCurAssets', 'CashEnd', 'CashEquiEnd', 'TotalCurLiab', 
                                                              'NonCurLiabIn1Y', 'NotesPayable', 'TaxesPayable', 'DepreciationFaOgaBba', 'AmortizationIntang']]
    finan_rept = finan_rept.set_index(['Date']).groupby(['Stkcd']).resample('Y').ffill().dropna().drop(['Stkcd'], axis=1)
    finan_rept.loc[:, 'STD'] = finan_rept['NonCurLiabIn1Y'] + finan_rept['NotesPayable']
    finan_rept.loc[:, 'DP'] = finan_rept['DepreciationFaOgaBba'] + finan_rept['AmortizationIntang']
    finan_rept.loc[:, 'CASH'] = finan_rept['CashEnd'] + finan_rept['CashEquiEnd']
    finan_rept['LastYearTotalAssets'] = finan_rept.groupby(['Stkcd'])['TotalAssets'].shift(1)
    finan_rept['LastYearTotalCurAssets'] = finan_rept.groupby(['Stkcd'])['TotalCurAssets'].shift(1)
    finan_rept['LastYearCASH'] = finan_rept.groupby(['Stkcd'])['CASH'].shift(1)
    finan_rept['LastYearTotalCurLiab'] = finan_rept.groupby(['Stkcd'])['TotalCurLiab'].shift(1)
    finan_rept['LastYearSTD'] = finan_rept.groupby(['Stkcd'])['STD'].shift(1)
    finan_rept['LastYearTaxesPayable'] = finan_rept.groupby(['Stkcd'])['TaxesPayable'].shift(1)
    
    finan_rept['Accrual'] = (finan_rept['TotalCurAssets'] - finan_rept['LastYearTotalCurAssets']) - \
                            (finan_rept['CASH'] - finan_rept['LastYearCASH']) - \
                            (finan_rept['TotalCurLiab'] - finan_rept['LastYearTotalCurLiab']) + \
                            (finan_rept['STD'] - finan_rept['LastYearSTD']) - \
                            (finan_rept['TaxesPayable'] - finan_rept['LastYearTaxesPayable']) - \
                            finan_rept['DP']
    finan_rept['AC'] = 2 * finan_rept['Accrual'] / (finan_rept['TotalAssets'] + finan_rept['LastYearTotalAssets'])

    ac = finan_rept.reset_index()[['Stkcd', 'Date', 'AC']].replace([np.inf, -np.inf], np.nan).dropna()
    to_today = ac.groupby(['Stkcd']).last().reset_index()
    to_today['Date'] = pd.to_datetime(this_month_begin)
    ac = pd.concat([ac, to_today], axis=0)
    ac = ac.set_index(['Date']).groupby(['Stkcd']).resample('M').ffill().dropna().drop(['Stkcd'], axis=1).reset_index()
    ac = pd.merge(ac, Stock_last_day, on=['Stkcd'])
    ac = ac[(ac.Date <= ac.LastDay)]
    ac = ac.drop(['LastDay'], axis=1)
    ac['Date'] = ac['Date'] + pd.offsets.MonthBegin(0)

    return ac


def upload(ac):
    ac.to_sql('AC', con=level3_factors, if_exists='replace', index=False)


@timeit('level3/AC')
def AC():
    data_bs, data_cfi, Stock_last_day = read_sql()
    ac = handle(data_bs, data_cfi, Stock_last_day)
    upload(ac)