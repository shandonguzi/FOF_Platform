
import time

import pandas as pd
from sqlalchemy import create_engine

from settings.database import DATABASE4, HOST0, PASSWD0, PORT0, USER0
from settings.jiayin_path import CH3_PATH


def read_CH3(path):
    return pd.read_pickle(path)

def handle_CH3(CH3):
    CH3['Date'] = pd.to_datetime(CH3[['year', 'month']].assign(day=1)) + pd.offsets.MonthEnd(0)
    CH3 = CH3.set_index('Date')
    CH3 = CH3[['rf', 'mktrf', 'smb', 'vmg']]
    return CH3

def upload_CH3(CH3):
    engine = create_engine(f'mysql://{USER0}:{PASSWD0}@{HOST0}:{PORT0}/{DATABASE4}')
    CH3.to_sql('m_CH3', con=engine, if_exists='replace')

def CH3():
    print(f'[+] {time.strftime("%c")} start level0/Jiayin CH3_Monthly')
    CH3 = read_CH3(CH3_PATH)
    CH3 = handle_CH3(CH3)
    upload_CH3(CH3)
    print(f'[+] {time.strftime("%c")} finish level0/Jiayin CH3_Monthly')
