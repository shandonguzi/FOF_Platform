
import pandas as pd
from settings.database import level0_joinquant, level1_csmar, level2_csmar

from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    Fund_Ptf_BondSpe = get_sql(level1_csmar, 'Fund_Ptf_BondSpe')
    Classification = get_sql(level0_joinquant, 'Fund_MainInfo')
    return Fund_Ptf_BondSpe, Classification

def handle(Fund_Ptf_BondSpe, Classification):
    bond_symbol = Classification[Classification.underlying_asset_type == '债券型'].Symbol.values
    Interest_Bond = Fund_Ptf_BondSpe[Fund_Ptf_BondSpe.Symbol.isin(bond_symbol)]
    Interest_Bond = Interest_Bond.groupby(['Date', 'Symbol']).apply(lambda x: x[x.SpeciesName.apply(lambda x: '国' in x or '金融' in x or '票据' in x)].Proportion.sum() > 50)
    Interest_Bond.name = 'Interest_Bond'
    Interest_Bond = Interest_Bond.reset_index()

    last_quarter_end = Interest_Bond.Date.nlargest(1).iloc[0]

    # delete when 12-31 data updated
    last_quarter_end = last_quarter_end - pd.offsets.QuarterEnd(1)
    
    this_quarter = Interest_Bond[Interest_Bond.Date == last_quarter_end].copy()

    # this_quarter['Date'] = last_quarter_end + pd.offsets.QuarterEnd(1)
    # delete when 12-31 data updated
    this_quarter['Date'] = last_quarter_end + pd.offsets.QuarterEnd(2)

    Interest_Bond = pd.concat([Interest_Bond, this_quarter])
    Interest_Bond = Interest_Bond.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').ffill()).drop('Symbol', axis=1)
    Interest_Bond = Interest_Bond[Interest_Bond.Interest_Bond]
    Interest_Bond = Interest_Bond.swaplevel()
    return Interest_Bond

def upload(Interest_Bond):
    Interest_Bond.to_sql('Interest_Bond', con=level2_csmar, if_exists='replace')

@timeit('level2/Interest_Bond')
def Interest_Bond():
    Fund_Ptf_BondSpe, Classification = read_sql()
    Interest_Bond = handle(Fund_Ptf_BondSpe, Classification)
    upload(Interest_Bond)
    