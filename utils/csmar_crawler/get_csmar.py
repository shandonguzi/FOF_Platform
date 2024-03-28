
import time

from utils.csmar_crawler.constants import SLEEP_TIME


def go_to_csmar(driver):
    '''
    open csmar website
    '''
    # driver.get('https://www.gtarsc.com/')
    driver.get('https://data.csmar.com/')
    print(f'[+] {time.strftime("%c")} going to csmar')
    time.sleep(SLEEP_TIME)
