
import numpy as np
import pandas as pd
from scipy import optimize as sco


def get_max_sharpe_weight(return_matrix):

    portfolio_returns = []
    portfolio_volatility = []
    portfolio_weights = []

    number_of_assets = return_matrix.shape[1]
    epochs = int(1e2)
    for epoch in np.arange(epochs):
        
        weights = np.random.random(number_of_assets)
        weights /= weights.sum()

        portfolio_returns.append(return_matrix.mean() @ weights)
        portfolio_volatility.append((weights @ return_matrix.corr() @ weights) ** .5)
        portfolio_weights.append(weights)

    portfolio_returns = np.array(portfolio_returns)
    portfolio_volatility = np.array(portfolio_volatility)
    sharpe_ratio = np.array(portfolio_returns / portfolio_volatility)

    number_of_assets = len(return_matrix.mean())

    def volatility(w, Σ):
        return (w @ Σ @ w)**.5

    bound = tuple((0, 1) for _ in range(number_of_assets))

    line_min = portfolio_returns.min() * 1.2 if portfolio_returns.min() < 0 else portfolio_returns.min() * (- 1.2)
    line_max = portfolio_returns.max() * 1.2 if portfolio_returns.max() > 0 else portfolio_returns.max() * (- 1.2)

    target_returns = np.linspace(line_min, line_max, 25)
    target_volatility = []
    target_weights = []

    for t_ret in target_returns:
        cons = ({'type': 'eq', 'fun': lambda x: return_matrix.mean() @ x - t_ret},
                {'type': 'eq', 'fun': lambda x: x.sum() - 1})
        initial_weights = number_of_assets * [1 / number_of_assets]
        result = sco.minimize(volatility, initial_weights, args = (return_matrix.corr()), bounds=bound, constraints=cons)
        target_volatility.append(result['fun'])
        target_weights.append(result['x'])

    target_sharpe_ratio = np.array(target_returns/ target_volatility)

    best_weights = target_weights[np.where(target_sharpe_ratio == target_sharpe_ratio.max())[0][0]]
    return pd.Series(best_weights, index=return_matrix.columns)

