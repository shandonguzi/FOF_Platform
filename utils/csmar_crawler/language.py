
import time

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

from utils.csmar_crawler.constants import SLEEP_TIME


def change_language(driver):

    i_frame = driver.find_element(By.TAG_NAME, 'iframe').get_attribute('src')

    driver.get(i_frame)
    print(f'[+] {time.strftime("%c")} got iframe')

    time.sleep(SLEEP_TIME)
    language_bar = driver.find_elements(By.XPATH, '//*[contains(text(), "En")]')[0]
    ac = ActionChains(driver)
    ac.move_to_element(language_bar).perform()
    time.sleep(SLEEP_TIME)
    print(f'[+] {time.strftime("%c")} expanded language bar')

    driver.find_elements(By.XPATH, '//*[contains(text(), "简体")]')[0].click()
    print(f'[+] {time.strftime("%c")} changing to Chinese')
    time.sleep(SLEEP_TIME)
