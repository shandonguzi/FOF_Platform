
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

    driver.find_elements(By.XPATH, '//*[contains(text(), "份额变动文件")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "期末总份额")]')[0].click()
    time.sleep(SLEEP_TIME)

def Fund_ShareChange():

    CHINESE_NAME = '份额变动文件'
    CODE = 'Fund_ShareChange'

    csmar_main(get_page, CHINESE_NAME, CODE)
    