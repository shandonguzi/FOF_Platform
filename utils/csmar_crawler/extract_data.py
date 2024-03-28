
import os
import time

from utils.get_md5 import get_md5

def rename_to_zip(CHINESE_NAME):
    try:
        os.system(f'mv /root/Downloads/download /root/Downloads/{CHINESE_NAME}.zip')
        print(f'[+] {time.strftime("%c")} rename binary to {CHINESE_NAME}.zip')
    except:
        print(f'[+] {time.strftime("%c")} do not rename binary')

def extract(CHINESE_NAME):
    rename_to_zip(CHINESE_NAME)
    os.system(f'unzip -qq -o /root/Downloads/*{CHINESE_NAME}*.zip -d /data/{CHINESE_NAME}')


def extract_data(CHINESE_NAME, CODE):

    file_path = f'/data/{CHINESE_NAME}/{CODE}.csv'
    if os.path.exists(file_path):
        previous = get_md5(file_path)
        extract(CHINESE_NAME)
        new = get_md5(file_path)
        same_file = previous == new
        if same_file:
            print(f'[+] {time.strftime("%c")} {CHINESE_NAME}/{CODE} did not update yesterday')
        return same_file

    else:
        print(f'[+] {time.strftime("%c")} init {CHINESE_NAME}/{CODE}')
        extract(CHINESE_NAME)
        return False

