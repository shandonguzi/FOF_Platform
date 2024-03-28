
from level4.top_funds import top_funds
from level4.top_funds_return import top_funds_return
from level4.portfolio_return import portfolio_return
from level4.portfolio_weight import portfolio_weight
from level4.vix_skew_integrated import vix_skew_integrated

from utils.time_function import timeit

def basic():
    top_funds(10)

def middle():
    top_funds_return()
    portfolio_weight()
    vix_skew_integrated()

def advanced():
    portfolio_return()

@timeit('Level 4')
def level4():
    basic()
    middle()
    advanced()
