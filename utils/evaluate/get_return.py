

def get_monthly_return(return_matrix):
    return_matrix = return_matrix.resample('M').last().pct_change()
    return return_matrix

def get_yearly_return(return_matrix):
    return_matrix = return_matrix.resample('y').last().pct_change()
    return return_matrix
