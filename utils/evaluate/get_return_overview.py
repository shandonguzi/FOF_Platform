
import pandas as pd

from settings.database import level1_csmar, level1_wind
from utils.frequent_dates import this_month_begin, last_month_end
from utils.mysql.get_sql import get_sql


def get_base_n_month_return(n, base):
    if base == 'TreasuryBond1D':
        base = get_sql(level1_wind, f'select * from {base}', index_cols='Date')
    else:
        base = get_sql(level1_csmar, f'select * from {base}_daily', index_cols='Date')
    base_return = (base.loc[this_month_begin - pd.offsets.MonthBegin(n): last_month_end] + 1).cumprod()[-1] - 1
    return base_return

def get_n_month_return(return_matrix, n):
    '''
    n = 1, last month begin to last month end
    n = 12, last year
    '''
    return_matrix = return_matrix.sort_index().loc[this_month_begin - pd.offsets.MonthBegin(n): last_month_end]
    return_matrix = (return_matrix + 1).prod() - 1
    return return_matrix


def get_n_month_annualized_return(return_matrix, n):
    n_month_return = get_n_month_return(return_matrix, n)
    n_month_annualized_return = (n_month_return + 1) ** (12 / n) - 1
    return n_month_annualized_return


def get_n_month_excess_return(return_matrix, n, base):
    '''
    base: hs300, sz50, zz500, TreasuryBond1D
    '''
    base_return = get_base_n_month_return(n, base)
    return_matrix = get_n_month_return(return_matrix, n)
    return_matrix -= base_return
    return return_matrix


def get_n_month_annualized_excess_return(return_matrix, n, base):
    n_month_excess_return = get_n_month_excess_return(return_matrix, n, base)
    n_month_annualized_excess_return = (n_month_excess_return + 1) ** (12 / n) - 1
    return n_month_annualized_excess_return
    

def get_yearly_return(return_matrix):
    years = return_matrix.index.year.drop_duplicates()
    results = []
    for year in years:
        result = (return_matrix.loc[str(year)] + 1).prod() - 1
        result.name = year
        results.append(result)

    results = pd.concat(results, axis=1).T
    return results

def get_overview(return_matrix, base):

    last_month = get_n_month_return(return_matrix, 1)
    last_3_month = get_n_month_return(return_matrix, 3)
    last_6_month = get_n_month_return(return_matrix, 6)
    last_year = get_n_month_return(return_matrix, 12)
    # last_2_year = get_n_month_return(return_matrix, 24)
    last_3_year = get_n_month_return(return_matrix, 36)
    # last_5_year = get_n_month_return(return_matrix, 60)

    last_month_excess = get_n_month_excess_return(return_matrix, 1, base)
    last_3_month_excess = get_n_month_excess_return(return_matrix, 3, base)
    last_6_month_excess = get_n_month_excess_return(return_matrix, 6, base)
    last_year_excess = get_n_month_excess_return(return_matrix, 12, base)
    # last_2_year_excess = get_n_month_excess_return(return_matrix, 24, base)
    last_3_year_excess = get_n_month_excess_return(return_matrix, 36, base)
    # last_5_year_excess = get_n_month_excess_return(return_matrix, 60, base)

    last_month_annualized = get_n_month_annualized_return(return_matrix, 1)
    last_3_month_annualized = get_n_month_annualized_return(return_matrix, 3)
    last_6_month_annualized = get_n_month_annualized_return(return_matrix, 6)
    last_year_annualized = get_n_month_annualized_return(return_matrix, 12)
    # last_2_year_annualized = get_n_month_annualized_return(return_matrix, 24)
    last_3_year_annualized = get_n_month_annualized_return(return_matrix, 36)
    # last_5_year_annualized = get_n_month_annualized_return(return_matrix, 60)

    last_month_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 1, base)
    last_3_month_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 3, base)
    last_6_month_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 6, base)
    last_year_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 12, base)
    # last_2_year_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 24, base)
    last_3_year_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 36, base)
    # last_5_year_excess_annualized = get_n_month_annualized_excess_return(return_matrix, 60, base)

    indexes = pd.MultiIndex.from_product(
            [['过去一月', '过去三月', '过去六月', '过去一年', '过去三年', ], ['收益率', '年化收益率', '超额收益率', '年化超额收益率']],
            names=['过去N月', '统计量']
            )

    overview = pd.DataFrame([last_month, last_month_annualized, last_month_excess, last_month_excess_annualized, 
    last_3_month, last_3_month_annualized, last_3_month_excess, last_3_month_excess_annualized, 
    last_6_month, last_6_month_annualized, last_6_month_excess, last_6_month_excess_annualized, 
    last_year, last_year_annualized, last_year_excess, last_year_excess_annualized, 
    # last_2_year, last_2_year_annualized, last_2_year_excess, last_2_year_excess_annualized, 
    last_3_year, last_3_year_annualized, last_3_year_excess, last_3_year_excess_annualized, 
    # last_5_year, last_5_year_annualized, last_5_year_excess, last_5_year_excess_annualized,
    ], 
    index=indexes,
    )
    return overview
