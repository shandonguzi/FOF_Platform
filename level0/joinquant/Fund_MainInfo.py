
import math
import time

import jqdatasdk as jq
import numpy as np
import pandas as pd

from settings.database import level0_joinquant
from settings.joinquant_auth import JOINQUANT_PASSWD, JOINQUANT_USER

def get_brief_of_funds() -> pd.DataFrame:
    return jq.get_all_securities(['fund', 'open_fund'], time.strftime('%Y%m%d'))

def get_all_fund_code():
    all_fund_brief = get_brief_of_funds()
    all_fund_code = all_fund_brief.index.str.split('.').str[0]
    return all_fund_code

def get_main_info(fund_codes) -> pd.DataFrame:
    limit = jq.DBTable.RESULT_ROWS_LIMIT
    fund_code_cuts = np.array_split(fund_codes, math.ceil(len(fund_codes) / limit))
    full_table_cuts = []
    for code_cut in fund_code_cuts:
        table_cut = jq.finance.run_query(jq.query(jq.finance.FUND_MAIN_INFO).filter(jq.finance.FUND_MAIN_INFO.main_code.in_(code_cut)))
        full_table_cuts.append(table_cut)
    full_table = pd.concat(full_table_cuts).reset_index(drop=True)
    return full_table

def get_all_fund_info():

    all_fund_info = get_main_info(get_all_fund_code())
    all_fund_info = all_fund_info[['main_code', 'name','underlying_asset_type', 'operate_mode', 'start_date']]
    all_fund_info = all_fund_info[all_fund_info.operate_mode == '开放式基金']

    all_fund_info = all_fund_info.rename(columns={'main_code': 'Symbol'})
    all_fund_info['Symbol'] = all_fund_info.Symbol.astype(int)
    all_fund_info['start_date'] = pd.to_datetime(all_fund_info.start_date)
    all_fund_info = all_fund_info.replace('添富快线B', '汇添富收益快线货币B')
    return all_fund_info

def upload(df):
    df.to_sql('Fund_MainInfo', level0_joinquant, if_exists='replace', index=False)

def JQ_Fund_MainInfo():
    print(f'[+] {time.strftime("%c")} start level0/JoinQuant data upload')
    jq.auth(JOINQUANT_USER, JOINQUANT_PASSWD)
    upload(get_all_fund_info())
    print(f'[=] {time.strftime("%c")} complete level0/JoinQuant data upload')
    print()
