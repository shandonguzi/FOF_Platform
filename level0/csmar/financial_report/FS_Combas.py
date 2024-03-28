
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
from utils.csmar_crawler.toggle_time import toggle_time
from utils.csmar_crawler.upload_data import upload_data
from utils.insure_successful_run import insure_successful_run


def get_page(driver):

    driver.find_elements(By.XPATH, '//*[contains(text(), "数据中心")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "公司研究系列")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[@title="财务报表"]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "保存")]')[-1].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "货币资金")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "存货净额")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "交易性金融资产")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "短期投资净额")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "流动资产合计")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "固定资产净额")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "资产总计")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "短期借款")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "交易性金融负债")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "应付票据")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "应交税费")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "一年内到期的非流动负债")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "长期借款")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "流动负债合计")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "少数股东权益")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[text()="所有者权益合计"]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "递延所得税资产")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "递延所得税负债")]')[0].click()
    time.sleep(SLEEP_TIME)

@insure_successful_run(5)
def FS_Combas():

    CHINESE_NAME = '资产负债表'
    CODE = 'FS_Combas'

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