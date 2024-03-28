
def one_time_fix_1(one_time):
    one_time.loc[one_time.nlargest(1).index] += 1 - one_time.sum()
    return one_time

def fix_not_1(matrix):
    matrix.apply(one_time_fix_1, axis=1)
    return matrix
    