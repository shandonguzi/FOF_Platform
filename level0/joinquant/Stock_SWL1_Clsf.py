
import time

import jqdatasdk as jq
import pandas as pd

from settings.database import level0_joinquant
from settings.joinquant_auth import JOINQUANT_PASSWD, JOINQUANT_USER


def jq_industry(jq_all_stocks_brief):
    jq_stock_classification = jq.get_industry(list(jq_all_stocks_brief.index), date=None)
    jq_stock_classification = pd.DataFrame(jq_stock_classification).loc['sw_l1'].dropna().apply(lambda info: info['industry_name'])
    jq_stock_classification = pd.DataFrame([jq_stock_classification.index.str.split('.').str[0], jq_stock_classification.values], index=['Stkcd', 'Indnme']).T
    jq_stock_classification['Stkcd'] = jq_stock_classification.Stkcd.astype(int)
    return jq_stock_classification

def upload(jq_stock_classification):
    jq_stock_classification.to_sql('Stock_SWL1_Clsf', level0_joinquant, if_exists='replace', index=False)

def JQ_Stock_SWL1_Clsf():
    print(f'[+] {time.strftime("%c")} start level0/JoinQuant/Stock_SWL1_Clsf data upload')
    jq.auth(JOINQUANT_USER, JOINQUANT_PASSWD)
    jq_all_stocks_brief = jq.get_all_securities(types=['stock'], date=None)
    upload(jq_industry(jq_all_stocks_brief))
    print(f'[=] {time.strftime("%c")} complete level0/JoinQuant/Stock_SWL1_Clsf data upload')
    print()
