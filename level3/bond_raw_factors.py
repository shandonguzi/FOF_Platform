
import numpy as np

from settings.database import level0_wind, level3_factors
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():

    TreasuryBond1Y = get_sql(level0_wind, 'TreasuryBond1Y', index_cols='Date')
    NationalBondIndex = get_sql(level0_wind, 'NationalBondIndex', index_cols='Date')
    CorpBondIndex = get_sql(level0_wind, 'CorpBondIndex', index_cols='Date')
    MBSIndex = get_sql(level0_wind, 'MBSIndex', index_cols='Date')
    AggregateIndex = get_sql(level0_wind, 'AggregateIndex', index_cols='Date')

    return TreasuryBond1Y, NationalBondIndex, CorpBondIndex, MBSIndex, AggregateIndex


def handle(TreasuryBond1Y, NationalBondIndex, CorpBondIndex, MBSIndex, AggregateIndex):

    TreasuryBond1Y = ((TreasuryBond1Y / 100 + 1) ** (1 / 12) - 1).resample('M').last()
    NationalBondIndexYield = NationalBondIndex.resample('M').last().pct_change()
    CorpBondIndexYield = CorpBondIndex.resample('M').last().pct_change()
    MBSIndexYield = MBSIndex.resample('M').last().pct_change()
    AggregateIndexYield = AggregateIndex.resample('M').last().pct_change()

    term_time = np.intersect1d(NationalBondIndexYield.index, TreasuryBond1Y.index)
    bond_term = NationalBondIndexYield.loc[term_time] - TreasuryBond1Y.loc[term_time]
    bond_term = bond_term.dropna()
    bond_term.name = 'bond_term'

    default_time = np.intersect1d(CorpBondIndexYield.index, NationalBondIndexYield.index)
    bond_default = CorpBondIndexYield.loc[default_time] - NationalBondIndexYield.loc[default_time]
    bond_default = bond_default.dropna()
    bond_default.name = 'bond_default'

    prepayment_time = np.intersect1d(MBSIndexYield.index, NationalBondIndexYield.index)
    bond_prepayment = MBSIndexYield.loc[prepayment_time] - NationalBondIndexYield.loc[prepayment_time]
    bond_prepayment = bond_prepayment.dropna()
    bond_prepayment.name = 'bond_prepayment'

    return bond_term, bond_default, bond_prepayment

def upload(bond_term, bond_default, bond_prepayment):
    bond_term.to_sql('bond_term', con=level3_factors, if_exists='replace')
    bond_default.to_sql('bond_default', con=level3_factors, if_exists='replace')
    bond_prepayment.to_sql('bond_prepayment', con=level3_factors, if_exists='replace')

@timeit('level3/bond_raw_factors')
def bond_raw_factors():
    TreasuryBond1Y, NationalBondIndex, CorpBondIndex, MBSIndex, AggregateIndex = read_sql()
    bond_term, bond_default, bond_prepayment = handle(TreasuryBond1Y, NationalBondIndex, CorpBondIndex, MBSIndex, AggregateIndex)
    upload(bond_term, bond_default, bond_prepayment)
