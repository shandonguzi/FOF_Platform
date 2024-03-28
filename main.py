from levels_hub.errata import errata
from levels_hub.level0 import level0
from levels_hub.level1 import level1
from levels_hub.level2 import level2
from levels_hub.level3 import level3
from levels_hub.level4 import level4
from levels_hub.patches import patches
from levels_hub.product import product
from levels_hub.statistics import statistics
from utils.time_function import timeit


def data():
    level0()
    level1()
    level2()
    level3()
    # level4()
    # errata()

def stat():
    statistics()

def sell():
    product()

@timeit('All')
def main():
    data()
    # stat()
    # sell()

main()
# patches()
