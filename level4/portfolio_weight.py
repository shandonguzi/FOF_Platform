
from settings.database import level4_factor_result
from settings.fund_select_project import *
from utils.factor_research.fix_not_1 import fix_not_1
from utils.fund_allocation_model.get_portfolio_weight import get_portfolio_weight
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql():
    top_funds_return = get_sql(level4_factor_result, 'top_funds_return', index_cols='Date')
    return top_funds_return

def handle(top_funds_return):

    total_9_funds_return = top_funds_return.filter(regex='[1-3]$')
    total_10_funds_return = top_funds_return.filter(regex='[1-3]$|Risky Fund Top 4')
    total_15_funds_return = top_funds_return.filter(regex='[1-5]$')
    total_30_funds_return = top_funds_return

    total_9_funds_weight_0 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=most_conservative_weight_bounds, λ=most_conservative_λ, γ=most_conservative_γ, mapper=mapper, lower=most_conservative_lower, upper=most_conservative_upper))
    total_9_funds_weight_1 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=moderate_conservative_weight_bounds, λ=moderate_conservative_λ, γ=moderate_conservative_γ, mapper=mapper, lower=moderate_conservative_lower, upper=moderate_conservative_upper))
    total_9_funds_weight_2 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=balanced_weight_bounds, λ=balanced_λ, γ=balanced_γ))
    total_9_funds_weight_3 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=moderate_risky_weight_bounds, λ=moderate_risky_λ, γ=moderate_risky_γ, mapper=mapper, lower=moderate_risky_lower, upper=moderate_risky_upper))
    total_9_funds_weight_4 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=most_risky_weight_bounds, λ=most_risky_λ, γ=most_risky_γ, mapper=mapper, lower=most_risky_lower, upper=most_risky_upper))
    total_9_funds_weight_5 = fix_not_1(get_portfolio_weight(total_9_funds_return, weight_bounds=extreme_weight_bounds, λ=extreme_λ, γ=extreme_γ, mapper=mapper, lower=extreme_lower, upper=extreme_upper))

    total_10_funds_weight_0 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=most_conservative_weight_bounds, λ=most_conservative_λ, γ=most_conservative_γ, mapper=mapper, lower=most_conservative_lower, upper=most_conservative_upper))
    total_10_funds_weight_1 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=moderate_conservative_weight_bounds, λ=moderate_conservative_λ, γ=moderate_conservative_γ, mapper=mapper, lower=moderate_conservative_lower, upper=moderate_conservative_upper))
    total_10_funds_weight_2 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=balanced_weight_bounds, λ=balanced_λ, γ=balanced_γ))
    total_10_funds_weight_3 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=moderate_risky_weight_bounds, λ=moderate_risky_λ, γ=moderate_risky_γ, mapper=mapper, lower=moderate_risky_lower, upper=moderate_risky_upper))
    total_10_funds_weight_4 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=most_risky_weight_bounds, λ=most_risky_λ, γ=most_risky_γ, mapper=mapper, lower=most_risky_lower, upper=most_risky_upper))
    total_10_funds_weight_5 = fix_not_1(get_portfolio_weight(total_10_funds_return, weight_bounds=extreme_weight_bounds, λ=extreme_λ, γ=extreme_γ, mapper=mapper, lower=extreme_lower, upper=extreme_upper))

    total_15_funds_weight_0 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=most_conservative_weight_bounds, λ=most_conservative_λ, γ=most_conservative_γ, mapper=mapper, lower=most_conservative_lower, upper=most_conservative_upper))
    total_15_funds_weight_1 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=moderate_conservative_weight_bounds, λ=moderate_conservative_λ, γ=moderate_conservative_γ, mapper=mapper, lower=moderate_conservative_lower, upper=moderate_conservative_upper))
    total_15_funds_weight_2 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=balanced_weight_bounds, λ=balanced_λ, γ=balanced_γ))
    total_15_funds_weight_3 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=moderate_risky_weight_bounds, λ=moderate_risky_λ, γ=moderate_risky_γ, mapper=mapper, lower=moderate_risky_lower, upper=moderate_risky_upper))
    total_15_funds_weight_4 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=most_risky_weight_bounds, λ=most_risky_λ, γ=most_risky_γ, mapper=mapper, lower=most_risky_lower, upper=most_risky_upper))
    total_15_funds_weight_5 = fix_not_1(get_portfolio_weight(total_15_funds_return, weight_bounds=extreme_weight_bounds, λ=extreme_λ, γ=extreme_γ, mapper=mapper, lower=extreme_lower, upper=extreme_upper))

    total_30_funds_weight_0 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=most_conservative_weight_bounds, λ=most_conservative_λ, γ=most_conservative_γ, mapper=mapper, lower=most_conservative_lower, upper=most_conservative_upper))
    total_30_funds_weight_1 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=moderate_conservative_weight_bounds, λ=moderate_conservative_λ, γ=moderate_conservative_γ, mapper=mapper, lower=moderate_conservative_lower, upper=moderate_conservative_upper))
    total_30_funds_weight_2 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=balanced_weight_bounds, λ=balanced_λ, γ=balanced_γ))
    total_30_funds_weight_3 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=moderate_risky_weight_bounds, λ=moderate_risky_λ, γ=moderate_risky_γ, mapper=mapper, lower=moderate_risky_lower, upper=moderate_risky_upper))
    total_30_funds_weight_4 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=most_risky_weight_bounds, λ=most_risky_λ, γ=most_risky_γ, mapper=mapper, lower=most_risky_lower, upper=most_risky_upper))
    total_30_funds_weight_5 = fix_not_1(get_portfolio_weight(total_30_funds_return, weight_bounds=extreme_weight_bounds, λ=extreme_λ, γ=extreme_γ, mapper=mapper, lower=extreme_lower, upper=extreme_upper))


    return [total_9_funds_weight_0, total_10_funds_weight_0, total_15_funds_weight_0, total_30_funds_weight_0], \
    [total_9_funds_weight_1, total_10_funds_weight_1, total_15_funds_weight_1, total_30_funds_weight_1], \
    [total_9_funds_weight_2, total_10_funds_weight_2, total_15_funds_weight_2, total_30_funds_weight_2], \
    [total_9_funds_weight_3, total_10_funds_weight_3, total_15_funds_weight_3, total_30_funds_weight_3], \
    [total_9_funds_weight_4, total_10_funds_weight_4, total_15_funds_weight_4, total_30_funds_weight_4], \
    [total_9_funds_weight_5, total_10_funds_weight_5, total_15_funds_weight_5, total_30_funds_weight_5]


def upload(most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme):

    for type_num, type_data_list in enumerate([most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme]):
        for one_data_in_type in list(zip(type_data_list, [9, 10, 15, 30])):
            weight = one_data_in_type[0]
            fund_num = one_data_in_type[1]
            weight.to_sql(f'total_{fund_num}_funds_weight_{type_num}', con=level4_factor_result, if_exists='replace')

@timeit('level4/portfolio_weight')
def portfolio_weight():
    top_funds_return = read_sql()
    most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme = handle(top_funds_return)
    upload(most_conservative, moderate_conservative, balanced, moderate_risky, most_risky, extreme)
