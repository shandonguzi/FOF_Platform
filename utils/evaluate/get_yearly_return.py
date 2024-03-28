
import pandas as pd

def get_yearly_return(return_matrix):
    years = return_matrix.index.year.drop_duplicates()
    results = []
    for year in years:
        result = (return_matrix.loc[str(year)] + 1).prod() - 1
        if type(result) == pd.Series:
            result.name = year
        results.append(result)
    if type(result) == pd.Series:
        results = pd.concat(results, axis=1).T
    else:
        results = pd.Series(results, index=years)
    results.index.name = 'year'
    return results
