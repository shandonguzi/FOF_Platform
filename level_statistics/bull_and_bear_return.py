
import imp
import time

import pandas as pd
from settings.database import errata_jiayin_robo_advisor, statistics_level4

from sqlalchemy import VARCHAR
from utils.evaluate.get_return_overview import get_certain_time_return
from utils.mysql.get_sql import get_sql


def read_sql():
    integrated_return = get_sql(errata_jiayin_robo_advisor, 'integrated_return', index_cols='Date')
    return integrated_return

def handle(integrated_return):
    integrated_return = integrated_return.drop('hs300', axis=1)
    bull0 = get_certain_time_return(integrated_return, 'hs300', '2014-10-1', '2015-6-1')
    bull0['market_status'] = 'bull'
    bull0 = bull0.set_index('market_status', append=True)
    bull1 = get_certain_time_return(integrated_return, 'hs300', '2017-6-1', '2018-2-1')
    bull1['market_status'] = 'bull'
    bull1 = bull1.set_index('market_status', append=True)
    bull2 = get_certain_time_return(integrated_return, 'hs300', '2020-6-1', '2021-2-1')
    bull2['market_status'] = 'bull'
    bull2 = bull2.set_index('market_status', append=True)

    bear0 = get_certain_time_return(integrated_return, 'hs300', '2015-6-1', '2016-3-1')
    bear0['market_status'] = 'bear'
    bear0 = bear0.set_index('market_status', append=True)
    bear1 = get_certain_time_return(integrated_return, 'hs300', '2018-2-1', '2018-12-1')
    bear1['market_status'] = 'bear'
    bear1 = bear1.set_index('market_status', append=True)
    bear2 = get_certain_time_return(integrated_return, 'hs300', '2022-1-1', '2022-11-1')
    bear2['market_status'] = 'bear'
    bear2 = bear2.set_index('market_status', append=True)


    bull_and_bear_return = pd.concat([bull0, bull1, bull2, bear0, bear1, bear2])
    bull_and_bear_return.index.names = ['category', 'market_status']
    bull_and_bear_return = bull_and_bear_return.set_index(['interval_end', 'interval_start'], append=True)

    return bull_and_bear_return

def upload(bull_and_bear_return):
    bull_and_bear_return.to_sql('bull_and_bear_return', con=statistics_level4, if_exists='replace', dtype={'category': VARCHAR(30), 'market_status': VARCHAR(10)})

def bull_and_bear_return():
    print(f'[+] {time.strftime("%c")} start level4/bull_and_bear_return')
    integrated_return = read_sql()
    bull_and_bear_return = handle(integrated_return)
    upload(bull_and_bear_return)
    print(f'[+] {time.strftime("%c")} finish level4/bull_and_bear_return')
    print()
    