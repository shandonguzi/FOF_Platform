
import os
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

from settings.csmar_crawler_options import get_options
from settings.database import level0_csmar
from utils.csmar_crawler.constants import SLEEP_TIME
from utils.csmar_crawler.download import download_data
from utils.csmar_crawler.extract_data import extract_data
from utils.csmar_crawler.get_csmar import go_to_csmar
from utils.csmar_crawler.language import change_language
from utils.csmar_crawler.toggle_time import toggle_time
from utils.insure_successful_run import insure_successful_run


def get_page(driver):

    driver.find_elements(By.XPATH, '//*[contains(text(), "数据中心")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "股票市场系列")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "股票市场交易")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "保存")]')[-1].click()
    time.sleep(SLEEP_TIME)
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "个股交易数据")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "日个股回报率文件")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "日收盘价")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "日个股流通市值")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "日个股总市值")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "考虑现金红利再投资的日个股回报率")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "涨跌幅")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "日个股交易股数")]')[0].click()
    time.sleep(SLEEP_TIME)

def upload_data(CHINESE_NAME, CODE, if_exists):
    '''
    multi-threaded data upload
    '''
    df = pd.read_csv(f'/data/{CHINESE_NAME}/{CODE}.csv', low_memory=False).dropna()
    df.to_sql(CODE, con=level0_csmar, index=False, if_exists=if_exists)

@insure_successful_run(5)
def TRD_Dalyr():

    CHINESE_NAME = '日个股回报率文件'
    CODE = 'TRD_Dalyr'

    os.system('rm -rf /root/Downloads/*')
    print(f'[+] {time.strftime("%c")} start level0/{CHINESE_NAME}/{CODE}')
    driver = webdriver.Chrome(options=get_options(webdriver))

    go_to_csmar(driver)
    change_language(driver)
    get_page(driver)
    toggle_time(driver)
    no_data = download_data(driver)
    if not no_data:
        did_not_update = extract_data(CHINESE_NAME, CODE)
        if not did_not_update:
            upload_data(CHINESE_NAME, CODE, if_exists='append')
    driver.close()
    print(f'[=] {time.strftime("%c")} complete level0/{CHINESE_NAME}/{CODE}')
    print()