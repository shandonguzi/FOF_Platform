
from settings.database import level0_wind, level1_wind
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit

def read_sql():
    '''basic io'''
    df = get_sql(level0_wind, 'TreasuryBond1Y')
    return df

def handle(df):
    df['TreasuryBond1D']= (df.TreasuryBond1Y + 1) ** (1 / 252) - 1
    series = df.set_index('Date').TreasuryBond1D
    return series

def upload(series):
    series.to_sql('TreasuryBond1D', con=level1_wind, if_exists='replace')

@timeit('level1/国债即期收益率:1日/TreasuryBond1D')
def TreasuryBond1D():
    df = read_sql()
    series = handle(df)
    upload(series)
