
import os
import time

from selenium import webdriver
from selenium.webdriver.common.by import By

from settings.csmar_crawler_options import get_options
from utils.csmar_crawler.constants import SLEEP_TIME
from utils.csmar_crawler.download import download_data
from utils.csmar_crawler.extract_data import extract_data
from utils.csmar_crawler.get_csmar import go_to_csmar
from utils.csmar_crawler.language import change_language
from utils.csmar_crawler.upload_data import upload_data
from utils.insure_successful_run import insure_successful_run


def get_page(driver):

    driver.find_elements(By.XPATH, '//*[contains(text(), "数据中心")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "基金市场系列")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "公募基金")]')[0].click()
    time.sleep(SLEEP_TIME)
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "保存")]')[-1].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "基金代码信息表")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "基金主代码")]')[0].click()
    time.sleep(SLEEP_TIME)

@insure_successful_run(5)
def FUND_FundCodeInfo():

    CHINESE_NAME = '基金代码信息表'
    CODE = 'FUND_FundCodeInfo'

    os.system(f'rm -rf /root/Downloads/{CHINESE_NAME}*')
    print(f'[+] {time.strftime("%c")} start level0/{CHINESE_NAME}/{CODE}')
    driver = webdriver.Chrome(options=get_options(webdriver))

    go_to_csmar(driver)
    change_language(driver)
    get_page(driver)
    no_data = download_data(driver)
    if not no_data:
        did_not_update = extract_data(CHINESE_NAME, CODE)
        if not did_not_update:
            upload_data(CHINESE_NAME, CODE, if_exists='append')
    driver.close()
    print(f'[=] {time.strftime("%c")} complete level0/{CHINESE_NAME}/{CODE}')
    print()
