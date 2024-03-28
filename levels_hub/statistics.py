
from level_statistics.return_overview import return_overview
from level_statistics.return_on_the_year import return_on_the_year
from level_statistics.advanced_statistics import advanced_statistics
from level_statistics.cumulative_return import cumulative_return
from level_statistics.get_selected_funds import get_selected_funds
# from level_statistics.bull_and_bear_return import bull_and_bear_return

from utils.time_function import timeit

@timeit('Statistics')
def statistics():
    return_overview()
    return_on_the_year()
    # bull_and_bear_return()
    advanced_statistics()
    cumulative_return()
    get_selected_funds()

if __name__ == '__main__':
    statistics()