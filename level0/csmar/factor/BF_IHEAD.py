
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

    # try:
    #     driver.find_elements(By.XPATH, '//*[contains(text(), "合作数据")]')[0].click()
    # except:
    #     pass
    # 解决在页面之外点击不到的问题

    driver.find_elements(By.XPATH, '//*[contains(text(), "因子研究系列")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "行为金融")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "保存")]')[-1].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "羊群效应")]')[0].click()
    time.sleep(SLEEP_TIME)

    # try:
    #     driver.find_elements(By.XPATH, '//*[contains(text(), "投资者情绪统计表")]')[0].click()
    #     time.sleep(SLEEP_TIME)
    #     print('投资者情绪统计表')
    # except:
    #     driver.save_screenshot('screenshot.png')
    #     print('already shot')
    # 截图查看错误原因

    driver.find_elements(By.XPATH, '//*[contains(text(), "股票行业羊群效应指标表(日)")]')[0].click()
    time.sleep(SLEEP_TIME)
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "考虑现金红利再投资的行业回报率(等权平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "考虑现金红利再投资的行业回报率(流通市值平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "考虑现金红利再投资的行业回报率(总市值平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)
    
    driver.find_elements(By.XPATH, '//*[contains(text(), "CSAD(考虑再投资等权平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)
        
    driver.find_elements(By.XPATH, '//*[contains(text(), "CSAD(考虑再投资流通市值平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)
        
    driver.find_elements(By.XPATH, '//*[contains(text(), "CSAD(考虑再投资总市值平均法)")]')[0].click()
    time.sleep(SLEEP_TIME)




def upload_data(CHINESE_NAME, CODE, if_exists):
    '''
    multi-threaded data upload
    '''
    df = pd.read_csv(f'/data/{CHINESE_NAME}/{CODE}.csv', low_memory=False).dropna()
    df.to_sql(CODE, con=level0_csmar, index=False, if_exists=if_exists)


@insure_successful_run(5)
def BF_IHEAD():

    CHINESE_NAME = '股票行业羊群效应指标表'
    CODE = 'BF_IHEAD'
    
    os.system(f"rm -rf /root/Downloads/{CHINESE_NAME}*")
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

