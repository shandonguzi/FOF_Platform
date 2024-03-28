

from errata.top_funds import top_funds
from errata.top_funds_return import top_funds_return
from errata.portfolio_return import portfolio_return
from errata.portfolio_weight import portfolio_weight
from errata.vix_skew_integrated import vix_skew_integrated

from utils.time_function import timeit

def basic():
    top_funds()

def middle():
    top_funds_return()
    portfolio_weight()
    vix_skew_integrated()

def advanced():
    portfolio_return()

@timeit('Errata')
def errata():
    basic()
    middle()
    advanced()
