

def get_options(webdriver):
    '''
    get chrome driver options
    '''
    options = webdriver.ChromeOptions()
    options.add_argument('--window-size=1920,1080')
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--lang=zh-CN")
    # options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--user-agent='Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.122 Mobile Safari/537.36 UCBrowser'")
    return options
