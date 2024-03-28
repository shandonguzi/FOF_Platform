


def get_certain_time_return(return_matrix, base, interval_start, interval_end):
    interval_start = pd.to_datetime(interval_start)
    interval_end = pd.to_datetime(interval_end)
    interval_length = round((interval_end - interval_start).days / 30)
    base = get_sql(level1_csmar, f'select * from {base}', index_cols='Date')
    base_return = get_monthly_return(base)
    base_return.index = base_return.index - pd.offsets.MonthBegin(1)
    base_return = base_return.loc[interval_start: interval_end]
    return_matrix = return_matrix.loc[interval_start: interval_end]

    mean_monthly_return = return_matrix.mean()
    base_mean_monthly_return = base_return.mean()

    std_mean_return = return_matrix.std()
    base_std_monthly_return = base_return.std()

    positive_excess_return_months = return_matrix.apply(lambda x: x > base_return).sum()
    positive_excess_return_months_proportion = positive_excess_return_months / interval_length

    interval_start = pd.Series([interval_start] * len(mean_monthly_return), index=mean_monthly_return.index)
    interval_end = pd.Series([interval_end] * len(mean_monthly_return), index=mean_monthly_return.index)
    interval_length = pd.Series([interval_length] * len(mean_monthly_return), index=mean_monthly_return.index)
    base_mean_monthly_return = pd.Series([base_mean_monthly_return] * len(mean_monthly_return), index=mean_monthly_return.index)
    base_std_monthly_return = pd.Series([base_std_monthly_return] * len(mean_monthly_return), index=mean_monthly_return.index)

    result = pd.concat([interval_start, interval_end, mean_monthly_return, base_mean_monthly_return, std_mean_return, base_std_monthly_return, interval_length, positive_excess_return_months, positive_excess_return_months_proportion], axis=1)
    result.columns = ['interval_start', 'interval_end', 'mean_monthly_return', 'base_mean_monthly_return', 'std_mean_return', 'base_std_monthly_return', 'interval_length', 'positive_excess_return_months', 'positive_excess_return_months_proportion']
    return result

