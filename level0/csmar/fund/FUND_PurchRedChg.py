
import time

from selenium.webdriver.common.by import By

from utils.csmar_crawler.constants import SLEEP_TIME
from utils.csmar_crawler.csmar_main import csmar_main


def get_page(driver):

    driver.find_elements(By.XPATH, '//*[contains(text(), "数据中心")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "基金市场系列")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "公募基金")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "保存")]')[-1].click()
    time.sleep(SLEEP_TIME)
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "基金申购赎回状态变更表")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "申购状态")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "赎回状态")]')[2].click()
    time.sleep(SLEEP_TIME)

def FUND_PurchRedChg():

    CHINESE_NAME = '基金申购赎回状态变更表'
    CODE = 'FUND_PurchRedChg'

    csmar_main(get_page, CHINESE_NAME, CODE)
    