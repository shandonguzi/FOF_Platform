
from settings.database import level2_csmar
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    '''basic io'''
    df_estimated_value = get_sql(level2_csmar, 'Fund_EstimatedValue')
    return df_estimated_value

def handle(df_estimated_value):
    EstimatedValue_m = df_estimated_value.groupby('Symbol').apply(lambda x: x.set_index('Date').resample('M').last().EstimatedValue.dropna()).swaplevel()
    EstimatedValue_m.name = 'EstimatedValue_m'
    return EstimatedValue_m

def upload(EstimatedValue_m):
    EstimatedValue_m.to_sql('Fund_EstimatedValue_m', con=level2_csmar, if_exists='replace')

@timeit('level2/Fund_EstimatedValue_m')
def Fund_EstimatedValue_m():
    df_estimated_value = read_sql()
    EstimatedValue_m = handle(df_estimated_value)
    upload(EstimatedValue_m)
