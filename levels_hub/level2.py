'''this module updates level2 data'''

from level2.Fund_EstimatedValue_m import Fund_EstimatedValue_m
from level2.Fund_EstimatedValue import Fund_EstimatedValue
from level2.Fund_NAV_adj import Fund_NAV_adj
from level2.Fund_Return_m import Fund_Return_m
from level2.Fund_Return import Fund_Return
from level2.Interest_Bond import Interest_Bond
from level2.Portfolio_Industry import Portfolio_Industry
from level2.Stock_Return_q import Stock_Return_q
from level2.Stock_Return_m_Ratio import Stock_Return_m_Ratio
from level2.Stock_last_day import Stock_last_day
from level2.Stock_Return_this_m import Stock_Return_this_m

from utils.time_function import timeit

def basic():
    Fund_NAV_adj()
    Fund_Return()
    Interest_Bond()
    # Portfolio_Industry()

def advanced():
    Fund_EstimatedValue()
    Fund_EstimatedValue_m()
    Fund_Return_m()

def stock():
    # Stock_Return_q()
    # Stock_Return_m_Ratio()
    Stock_last_day()
    Stock_Return_this_m()

@timeit('Level 2')
def level2():
    
    basic()
    advanced()
    stock()

