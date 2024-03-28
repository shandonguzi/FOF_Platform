from levels_hub.errata import errata
from levels_hub.level0 import level0
from levels_hub.level1 import level1
from levels_hub.level2 import level2
from levels_hub.level3 import *
from levels_hub.level4 import level4
from levels_hub.patches import patches
from levels_hub.product import product
from levels_hub.statistics import statistics
from utils.time_function import timeit

from product.daily_update import daily_update
from product.monthly_update import monthly_update



def level3_monthly():

    fundamental_stock_factors() # monthly
    Stock_to_Fund() # monthly
    fundamental_fund_factors() # monthly
    ipca_score() # monthly


@timeit('monthly')
def main():
    # 每月运行level3_monthly，level4，errata和product里的日和月update
    level3_monthly()
    level4()
    errata()

    # product:
    daily_update()
    monthly_update()

if __name__ == '__main__':
    main()


