from patches.may2023.data_source_interim import data_source_interim
from utils.time_function import timeit

@timeit('All')
def patches():
    data_source_interim()
