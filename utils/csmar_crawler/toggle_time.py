
import time

import pandas as pd
from selenium.webdriver.common.by import By

from utils.csmar_crawler.constants import SLEEP_TIME
from utils.frequent_dates import last_month_s, yesterday, yesterday_s


def toggle_time(driver):
    '''
    change the download time span
    '''
    print(f"[+] start toggle time")
    start_date = driver.find_elements(By.XPATH, '//*[@class="date-range-picker table-picker"]//input')[0]
    end_date = driver.find_elements(By.XPATH, '//*[@class="date-range-picker table-picker"]//input')[1]
    last_available_day = driver.find_elements(By.XPATH, '//*[@class="el-date-editor el-input el-input--medium el-input--prefix el-input--suffix el-date-editor--date"]')[0].get_attribute('end')

    if pd.to_datetime(last_available_day) < yesterday:
        start_date.clear()
        start_date.send_keys(pd.to_datetime(last_available_day).strftime('%Y-%m-%d'))
        print(f"[=] from {pd.to_datetime(last_available_day).strftime('%Y-%m-%d')} to {pd.to_datetime(last_available_day).strftime('%Y-%m-%d')}")

    else:
        start_date.clear()
        start_date.send_keys(yesterday_s)
        time.sleep(SLEEP_TIME)
        end_date.clear()
        end_date.send_keys(yesterday_s)
        print(f'[=] {time.strftime("%c")} from {yesterday_s} to {yesterday_s}')

    time.sleep(SLEEP_TIME)


def toggle_time_m(driver):

    start_date = driver.find_elements(By.XPATH, '//*[@class="date-range-picker table-picker"]//input')[0]
    end_date = driver.find_elements(By.XPATH, '//*[@class="date-range-picker table-picker"]//input')[1]

    start_date.clear()
    start_date.send_keys(last_month_s)
    time.sleep(SLEEP_TIME)
    end_date.clear()
    end_date.send_keys(last_month_s)
    print(f"[=] from {last_month_s} to {last_month_s}")

    time.sleep(SLEEP_TIME)
