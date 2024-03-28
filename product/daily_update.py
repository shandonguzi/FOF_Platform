
import time

from settings.database import PRODUCT0, errata_jiayin_robo_advisor, product_jiayin_robo_advisor
from utils.mysql.get_sql import get_sql
from utils.frequent_dates import today_s

def read_sql():
    advise_on_entry = get_sql(errata_jiayin_robo_advisor, f'select * from advise_on_entry where Date="{today_s}"', index_cols='Date')
    advise_on_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from advise_on_rebalance where Date="{today_s}"', index_cols='Date')

    total_9_funds_weight_2_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_9_funds_weight_2_rebalance where Date="{today_s}"', index_cols='Date')
    total_9_funds_weight_3_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_9_funds_weight_3_rebalance where Date="{today_s}"', index_cols='Date')
    total_9_funds_weight_4_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_9_funds_weight_4_rebalance where Date="{today_s}"', index_cols='Date')

    total_10_funds_weight_2_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_10_funds_weight_2_rebalance where Date="{today_s}"', index_cols='Date')
    total_10_funds_weight_3_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_10_funds_weight_3_rebalance where Date="{today_s}"', index_cols='Date')
    total_10_funds_weight_4_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_10_funds_weight_4_rebalance where Date="{today_s}"', index_cols='Date')

    total_15_funds_weight_2_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_15_funds_weight_2_rebalance where Date="{today_s}"', index_cols='Date')
    total_15_funds_weight_3_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_15_funds_weight_3_rebalance where Date="{today_s}"', index_cols='Date')
    total_15_funds_weight_4_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_15_funds_weight_4_rebalance where Date="{today_s}"', index_cols='Date')

    total_30_funds_weight_2_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_30_funds_weight_2_rebalance where Date="{today_s}"', index_cols='Date')
    total_30_funds_weight_3_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_30_funds_weight_3_rebalance where Date="{today_s}"', index_cols='Date')
    total_30_funds_weight_4_rebalance = get_sql(errata_jiayin_robo_advisor, f'select * from total_30_funds_weight_4_rebalance where Date="{today_s}"', index_cols='Date')

    total_9 = [total_9_funds_weight_2_rebalance, total_9_funds_weight_3_rebalance, total_9_funds_weight_4_rebalance]
    total_10 = [total_10_funds_weight_2_rebalance, total_10_funds_weight_3_rebalance, total_10_funds_weight_4_rebalance]
    total_15 = [total_15_funds_weight_2_rebalance, total_15_funds_weight_3_rebalance, total_15_funds_weight_4_rebalance]
    total_30 = [total_30_funds_weight_2_rebalance, total_30_funds_weight_3_rebalance, total_30_funds_weight_4_rebalance]

    return advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30

def handle(advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30):

    pass

    return advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30

def upload(advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30):

    advise_on_rebalance.to_sql('advise_on_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    advise_on_entry.to_sql('advise_on_entry', con=product_jiayin_robo_advisor, if_exists='append')

    total_9[0].to_sql('total_9_funds_weight_2_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[1].to_sql('total_9_funds_weight_3_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[2].to_sql('total_9_funds_weight_4_rebalance', con=product_jiayin_robo_advisor, if_exists='append')

    total_10[0].to_sql('total_10_funds_weight_2_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[1].to_sql('total_10_funds_weight_3_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[2].to_sql('total_10_funds_weight_4_rebalance', con=product_jiayin_robo_advisor, if_exists='append')

    total_15[0].to_sql('total_15_funds_weight_2_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[1].to_sql('total_15_funds_weight_3_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[2].to_sql('total_15_funds_weight_4_rebalance', con=product_jiayin_robo_advisor, if_exists='append')

    total_30[0].to_sql('total_30_funds_weight_2_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[1].to_sql('total_30_funds_weight_3_rebalance', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[2].to_sql('total_30_funds_weight_4_rebalance', con=product_jiayin_robo_advisor, if_exists='append')


def daily_update():
    print(f'[+] {time.strftime("%c")} start {PRODUCT0}/daily_update')
    advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30 = read_sql()
    advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30 = handle(advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30)
    upload(advise_on_entry, advise_on_rebalance, total_9, total_10, total_15, total_30)
    print(f'[+] {time.strftime("%c")} finish {PRODUCT0}/daily_update')
    print()
