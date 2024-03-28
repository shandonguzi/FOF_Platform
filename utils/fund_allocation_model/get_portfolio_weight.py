
import pandas as pd

from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import objective_functions

from utils.insure_successful_run import insure_successful_run

def get_portfolio_weight(raw_portfolio_return, weight_bounds, λ, γ, mapper=None, lower=None, upper=None):
    '''
    :type weight_bounds: tuple OR tuple list, optional
    :type risk_aversion: positive float
    :meaning risk_aversion: bigger and more averse
    '''
    raw_portfolio_return = raw_portfolio_return.dropna()
    portfolio_price_matrix = (raw_portfolio_return + 1).cumprod()
    portfolio_price_matrix.index = portfolio_price_matrix.index + pd.offsets.MonthEnd(0)
    ų_time_series = raw_portfolio_return.expanding(min_periods=2).apply(lambda x: (1 + x).prod() ** (12 / x.count()) - 1)
    ų_time_series.index = ų_time_series.index + pd.offsets.MonthBegin(1)
    results = []
    portfolio_price_matrix.iloc[:, 0].rolling(12).apply(lambda x: rolling_portfolio(x, results, ų_time_series, portfolio_price_matrix, weight_bounds, λ, γ, mapper, lower, upper))
    results = pd.DataFrame(results)
    results.index.name = 'Date'
    return results


def get_shrunken_Σ(price_matrix):
    Σ = risk_models.CovarianceShrinkage(price_matrix).ledoit_wolf()
    return Σ

@insure_successful_run(3)
def get_one_time_portfolio(ų, Σ, γ, time, weight_bounds, λ, mapper, lower, upper):

    model = EfficientFrontier(ų, Σ, weight_bounds=weight_bounds)
    model.add_objective(objective_functions.L2_reg, gamma=γ)
    if mapper != None and lower != None and upper != None:
        model.add_sector_constraints(mapper, lower, upper)
    one_time_portfolio = pd.Series(model.max_quadratic_utility(risk_aversion=λ).values(), index=ų.index, name=time[-1] + pd.offsets.MonthBegin(1))
    return one_time_portfolio


def rolling_portfolio(x, results, ų_time_series, portfolio_price_matrix, weight_bounds, λ, γ, mapper, lower, upper):
    '''
    以下例子为帮助理清时间戳，以保证不出现时间错配问题

    例
    1, 2, 3月的数据计算4月1日weight
    那么time应是 1-31, 2-28, 3-31
    price_matrix为1-1~1-31累乘价格、2-1~2-28累乘价格、3-1~3-31累乘价格, 时间戳为1-31, 2-28, 3-31
    Σ为上述price_matrix协方差矩阵 仅使用1, 2, 3月历史信息计算
    ų为1-1~1-31收益率、2-1~2-28收益率、3-1~3-31收益率三者平均值, 但时间戳为4-1
    带入get_one_time_portfolio后, 即得到4-1应配置的weight
    '''
    
    time = x.index
    Σ = get_shrunken_Σ(portfolio_price_matrix.loc[time])
    ų = ų_time_series.loc[time[-1] + pd.offsets.MonthBegin(0)]
    one_time_portfolio = get_one_time_portfolio(ų, Σ, γ, time, weight_bounds, λ, mapper, lower, upper)
    results.append(one_time_portfolio)
    return 0
