
from product.daily_update import daily_update
from product.monthly_update import monthly_update
from utils.time_function import timeit


@timeit('Product')
def product():
    daily_update()
    # monthly_update()
