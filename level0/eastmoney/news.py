
import time

print(f'[+] {time.strftime("%c")} start crawling news...')

import sys

sys.path.append('/code')

import datetime

import numpy as np
import pandas as pd
import redis
from selenium import webdriver
from selenium.webdriver.common.by import By

from settings.database import level0_eastmoney
from utils.mysql.get_sql import get_sql

LOOKBACK = 90

connection = redis.Redis(host='proxy_redis', password='abc.123#', port=6379, db=0)

all_proxies = list(map(lambda byte: byte.decode("utf-8"), connection.zrange('proxies:eastmoney', 0, -1)))

print(time.strftime('%c'))
available_proxies = []

if len(all_proxies) == 0:
    print('[!] Currently not available')

def get_options(webdriver):
    options = webdriver.ChromeOptions()
    options.add_argument('--window-size=1920,1080')
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--lang=zh-CN")
    # options.add_argument('--disable-dev-shm-usage')
    sampled_proxy = np.random.choice(all_proxies, size=1)[0]
    # 此处为代理覆写，后续需要改为正式的代理池
    sampled_proxy = np.random.choice(['10.8.3.37:8787', 'dacian.cc:65534'], size=1)[0]
    used_proxy = f'--proxy-server=http://{sampled_proxy}'
    print(f'[=] {time.strftime("%c")} {used_proxy}')
    options.add_argument(used_proxy)
    options.add_argument("--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.58'")
    return options

def handle(driver):
    print(f'[+] {time.strftime("%c")} locating news hub...')
    news_list = driver.find_element(by=By.XPATH, value='//*[@id="livenews-list"]')
    print(f'[=] {time.strftime("%c")} located news hub')

    print(f'[+] {time.strftime("%c")} locating news...')
    news_items = news_list.find_elements(By.XPATH, './/div[contains(@class, "livenews-media")]')
    print(f'[=] {time.strftime("%c")} located {len(news_items)} news')

    news = []

    print(f'[+] {time.strftime("%c")} memorizing news...')
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
    print(f'[=] {time.strftime("%c")} memorized {len(news)} news in previous {LOOKBACK} minutes')

    return news

def news():
    print(f'[+] {time.strftime("%c")} launching chrome...')
    driver = webdriver.Chrome(options=get_options(webdriver))
    print(f'[=] {time.strftime("%c")} launched')

    print(f'[+] {time.strftime("%c")} going to https://kuaixun.eastmoney.com')
    # driver.get('https://baidu.com/')
    # driver.save_screenshot('screenshot_baidu.png')
    # time.sleep(10)
    driver.get('https://kuaixun.eastmoney.com/')
    print(f'[=] {time.strftime("%c")} got there')
    time.sleep(10)
    # driver.save_screenshot('screenshot_es.png')

    print(f'[+] {time.strftime("%c")} analyzing news...')
    news = handle(driver)
    print(f'[=] {time.strftime("%c")} got news with shape {news.shape}')

    print(f'[+] {time.strftime("%c")} exporting to database...')
    check_time_start = (pd.Timestamp.now() - pd.Timedelta(minutes=LOOKBACK + 30)).strftime('%Y-%m-%d %H:%M')
    check_time_end = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    previous = get_sql(level0_eastmoney, f'select * from news where Date between "{check_time_start}" and "{check_time_end}"', index_cols='Date')
    news = news[~ news.News.isin(previous)]
    print(f'[+] {time.strftime("%c")} exporting {len(news)} news')
    news.to_sql('news', con=level0_eastmoney, if_exists='append')
    print(f'[=] {time.strftime("%c")} exported {len(news)} news')
    print(f'[=] {time.strftime("%c")} done\n')

news()
