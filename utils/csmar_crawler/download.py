'''Module used to download data'''

import os
import time

from selenium.webdriver.common.by import By

from utils.csmar_crawler.constants import SLEEP_TIME


def check_no_data_and_download(driver):
    
    all_try = 300

    while all_try > 0:
        no_data = driver.find_elements(By.XPATH, '//*[contains(text(), "当前查询结果为0条，请重新设置查询条件")]')
        save = driver.find_elements(By.XPATH, '//*[contains(text(), "本地保存数据")]')
        if len(no_data) + len(save) == 0:
            all_try -= 1
            time.sleep(1)
        else:
            if not no_data:
                driver.find_elements(By.XPATH, '//*[contains(text(), "本地保存数据")]')[0].click()
                print(f'[+] {time.strftime("%c")} clicked save data')
                no_data = False
            else:
                no_data = True
                print(f'[=] {time.strftime("%c")} expected no data')
            return no_data

    if all_try == 0:
        no_data = True
        print(f'[×] {time.strftime("%c")} {time.strftime("%c")} 300 tries no data')
        return no_data

def download_wait(path_to_downloads):

    waits = 300
    seconds = 0
    not_downloaded = True

    while not_downloaded and seconds < waits:
        print(f'[+] {time.strftime("%c")} downloading')
        time.sleep(1)
        if len(os.listdir(path_to_downloads)) != 0:
            not_downloaded = False

        for fname in os.listdir(path_to_downloads):
            if fname.endswith('.crdownload'):
                not_downloaded = True
        seconds += 1
    if not_downloaded:
        print(f'[×] {time.strftime("%c")} 300 seconds {time.strftime("%c")} not downloaded')

    print(f'[+] {time.strftime("%c")} downloaded data')
    return not_downloaded
    

def download_data(driver):

    driver.find_elements(By.XPATH, '//*[contains(text(), "CSV格式（*.csv）")]')[0].click()
    time.sleep(SLEEP_TIME)

    driver.find_elements(By.XPATH, '//*[contains(text(), "下载数据")]')[-1].click()
    time.sleep(SLEEP_TIME)

    driver.switch_to.window(driver.window_handles[-1])

    no_data = check_no_data_and_download(driver)

    if not no_data:
        not_downloaded = download_wait('/root/Downloads')
    else:
        not_downloaded = True

    driver.close()
    time.sleep(SLEEP_TIME)
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(SLEEP_TIME)

    return no_data and not_downloaded
