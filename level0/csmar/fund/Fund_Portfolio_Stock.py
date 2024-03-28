
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
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "基金投组")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "股票投资明细表")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "占净值比例(%)")]')[0].click()
    time.sleep(SLEEP_TIME)

def Fund_Portfolio_Stock():

    CHINESE_NAME = '股票投资明细表'
    CODE = 'Fund_Portfolio_Stock'

    csmar_main(get_page, CHINESE_NAME, CODE)
    