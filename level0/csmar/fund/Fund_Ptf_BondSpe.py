
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

    driver.find_elements(By.XPATH, '//*[contains(text(), "按品种分类的债券投资组合")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "占基金资产净值比例(%)")]')[0].click()
    time.sleep(SLEEP_TIME)

def Fund_Ptf_BondSpe():

    CHINESE_NAME = '按品种分类的债券投资组合'
    CODE = 'Fund_Ptf_BondSpe'

    csmar_main(get_page, CHINESE_NAME, CODE)
    