
import time

import jqdatasdk as jq
import pandas as pd

from settings.database import level0_joinquant
from settings.joinquant_auth import JOINQUANT_PASSWD, JOINQUANT_USER


def jq_CSRC_Clsf(jq_all_stocks_brief):
    jq_stock_CSRC_Clsf = jq.get_industry(list(jq_all_stocks_brief.index), date=None)
    jq_stock_CSRC_Clsf = pd.DataFrame(jq_stock_CSRC_Clsf).loc['zjw'].dropna()
    jq_stock_CSRC_Clsf_values = pd.Series(jq_stock_CSRC_Clsf.values).apply(pd.Series)[['industry_code', 'industry_name']]
    jq_stock_CSRC_Clsf_values['Stkcd'] = pd.Series(jq_stock_CSRC_Clsf.index.str.split('.').str[0])
    jq_stock_CSRC_Clsf_values['Stkcd'] = jq_stock_CSRC_Clsf_values.Stkcd.astype(int)
    return jq_stock_CSRC_Clsf_values

def upload(jq_stock_classification):
    jq_stock_classification.to_sql('Stock_CSRC_Clsf', level0_joinquant, if_exists='replace', index=False)

def JQ_Stock_CSRC_Clsf():
    print(f'[+] {time.strftime("%c")} start level0/JoinQuant/Stock_CSRC_Clsf data upload')
    jq.auth(JOINQUANT_USER, JOINQUANT_PASSWD)
    jq_all_stocks_brief = jq.get_all_securities(types=['stock'], date=None)
    upload(jq_CSRC_Clsf(jq_all_stocks_brief))
    print(f'[=] {time.strftime("%c")} complete level0/JoinQuant/Stock_CSRC_Clsf data upload')
    print()
