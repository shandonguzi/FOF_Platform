
import time

print(f'[+] {time.strftime("%c")} start checking proxies...')
import sys

sys.path.append('/code')

import time

import numpy as np
from selenium import webdriver

start_time = time.time()
import datetime
import sys
from collections import deque

import pandas as pd
import redis
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm

LOOKBACK = 10

connection = redis.Redis(host='proxy_redis', password='abc.123#', port=6379, db=0)

all_proxies = deque(set(map(lambda byte: byte.decode("utf-8"), connection.zrange('proxies:universal', 0, -1))))

print(time.strftime('%c'))
available_proxies = []

if len(all_proxies) == 0:
    print('[!] Currently not available')

proxies_for_sampling = np.random.choice(all_proxies, size=1000)

try:
    past_proxies = deque(set(map(lambda byte: byte.decode("utf-8"), connection.zrange('proxies:eastmoney', 0, -1))))

    proxies_for_sampling += deque(past_proxies)
    proxies_for_sampling = deque(set(proxies_for_sampling))
except:
    print(f'[!] No past proxies')

def get_options(webdriver, proxy_check):
    options = webdriver.ChromeOptions()
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--no-sandbox')
    options.add_argument("--lang=zh-CN")
    options.add_argument(f'--proxy-server=http://{proxy_check}')
    options.add_argument("--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.58'")
    return options


def handle(driver):
    news_list = driver.find_element(by=By.XPATH, value='//*[@id="livenews-list"]')

    news_items = news_list.find_elements(By.XPATH, './/div[contains(@class, "livenews-media")]')

    news = []
    for item in news_items:
        timestamp = item.find_element(By.XPATH, './/div/span').get_attribute('textContent')
        news_content = item.find_element(By.XPATH, './/div/h2/a').get_attribute('textContent')

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d ') + timestamp
        news.append({
            'Date': timestamp,
            'News': news_content
        })
    news = pd.DataFrame(news)
    news = news.set_index('Date')
    news.index = pd.to_datetime(news.index, format='%Y-%m-%d %H:%M')
    news = news.loc[news.index > pd.Timestamp.now() - pd.Timedelta(minutes=LOOKBACK)]

    return news

def main(all_proxies_for_sampling):
    print(f'[+] {time.strftime("%c")} launching chrome...')

    for proxy_check in tqdm(all_proxies_for_sampling):
        driver = webdriver.Chrome(options=get_options(webdriver, proxy_check))
        driver.set_page_load_timeout(30)
        try:
            driver.get('https://kuaixun.eastmoney.com/')
            handle(driver)
            connection.zadd('proxies:eastmoney', {proxy_check: 1})
            print(f'{proxy_check} added')
        except:
            driver.close()
            try:
                connection.zrem('proxies:eastmoney', proxy_check)
            except:
                pass

main(proxies_for_sampling)
