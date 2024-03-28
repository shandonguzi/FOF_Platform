
import time

from settings.database import PRODUCT0, level4_factor_result, product_jiayin_robo_advisor, errata_jiayin_robo_advisor
from utils.mysql.get_sql import get_sql
from utils.frequent_dates import last_month_begin, next_month_begin, this_month_begin, base_date

def right_date_return():
    if base_date.day < 15:
        return last_month_begin
    else:
        return this_month_begin

def right_date_weight():
    if base_date.day < 15:
        return this_month_begin
    else:
        return next_month_begin

date_return = right_date_return()
date_weight = right_date_weight()
def read_sql():
    
    funds_code = get_sql(level4_factor_result, f'select * from top_funds where Date="{date_weight}"', index_cols='Date')
    funds_name = get_sql(level4_factor_result, f'select * from top_funds_name where Date="{date_weight}"', index_cols='Date')

    funds_return_0 = get_sql(errata_jiayin_robo_advisor, f'select * from funds_return_0 where Date="{date_return}"', index_cols='Date')
    funds_return_1 = get_sql(errata_jiayin_robo_advisor, f'select * from funds_return_1 where Date="{date_return}"', index_cols='Date')
    funds_return_2 = get_sql(errata_jiayin_robo_advisor, f'select * from funds_return_2 where Date="{date_return}"', index_cols='Date')
    funds_return_3 = get_sql(errata_jiayin_robo_advisor, f'select * from funds_return_3 where Date="{date_return}"', index_cols='Date')
    funds_return_4 = get_sql(errata_jiayin_robo_advisor, f'select * from funds_return_4 where Date="{date_return}"', index_cols='Date')

    top_funds_return = get_sql(errata_jiayin_robo_advisor, f'select * from top_funds_return where Date="{date_return}"', index_cols='Date')
    
    total_9_funds_weight_0 = get_sql(level4_factor_result, f'select * from total_9_funds_weight_0 where Date="{date_weight}"', index_cols='Date')
    total_9_funds_weight_1 = get_sql(level4_factor_result, f'select * from total_9_funds_weight_1 where Date="{date_weight}"', index_cols='Date')
    total_9_funds_weight_2 = get_sql(level4_factor_result, f'select * from total_9_funds_weight_2 where Date="{date_weight}"', index_cols='Date')
    total_9_funds_weight_3 = get_sql(level4_factor_result, f'select * from total_9_funds_weight_3 where Date="{date_weight}"', index_cols='Date')
    total_9_funds_weight_4 = get_sql(level4_factor_result, f'select * from total_9_funds_weight_4 where Date="{date_weight}"', index_cols='Date')
    
    total_10_funds_weight_0 = get_sql(level4_factor_result, f'select * from total_10_funds_weight_0 where Date="{date_weight}"', index_cols='Date')
    total_10_funds_weight_1 = get_sql(level4_factor_result, f'select * from total_10_funds_weight_1 where Date="{date_weight}"', index_cols='Date')
    total_10_funds_weight_2 = get_sql(level4_factor_result, f'select * from total_10_funds_weight_2 where Date="{date_weight}"', index_cols='Date')
    total_10_funds_weight_3 = get_sql(level4_factor_result, f'select * from total_10_funds_weight_3 where Date="{date_weight}"', index_cols='Date')
    total_10_funds_weight_4 = get_sql(level4_factor_result, f'select * from total_10_funds_weight_4 where Date="{date_weight}"', index_cols='Date')

    total_15_funds_weight_0 = get_sql(level4_factor_result, f'select * from total_15_funds_weight_0 where Date="{date_weight}"', index_cols='Date')
    total_15_funds_weight_1 = get_sql(level4_factor_result, f'select * from total_15_funds_weight_1 where Date="{date_weight}"', index_cols='Date')
    total_15_funds_weight_2 = get_sql(level4_factor_result, f'select * from total_15_funds_weight_2 where Date="{date_weight}"', index_cols='Date')
    total_15_funds_weight_3 = get_sql(level4_factor_result, f'select * from total_15_funds_weight_3 where Date="{date_weight}"', index_cols='Date')
    total_15_funds_weight_4 = get_sql(level4_factor_result, f'select * from total_15_funds_weight_4 where Date="{date_weight}"', index_cols='Date')

    total_30_funds_weight_0 = get_sql(level4_factor_result, f'select * from total_30_funds_weight_0 where Date="{date_weight}"', index_cols='Date')
    total_30_funds_weight_1 = get_sql(level4_factor_result, f'select * from total_30_funds_weight_1 where Date="{date_weight}"', index_cols='Date')
    total_30_funds_weight_2 = get_sql(level4_factor_result, f'select * from total_30_funds_weight_2 where Date="{date_weight}"', index_cols='Date')
    total_30_funds_weight_3 = get_sql(level4_factor_result, f'select * from total_30_funds_weight_3 where Date="{date_weight}"', index_cols='Date')
    total_30_funds_weight_4 = get_sql(level4_factor_result, f'select * from total_30_funds_weight_4 where Date="{date_weight}"', index_cols='Date')

    funds_returns = [funds_return_0, funds_return_1, funds_return_2, funds_return_3, funds_return_4]
    total_9 = [total_9_funds_weight_0, total_9_funds_weight_1, total_9_funds_weight_2, total_9_funds_weight_3, total_9_funds_weight_4]
    total_10 = [total_10_funds_weight_0, total_10_funds_weight_1, total_10_funds_weight_2, total_10_funds_weight_3, total_10_funds_weight_4]
    total_15 = [total_15_funds_weight_0, total_15_funds_weight_1, total_15_funds_weight_2, total_15_funds_weight_3, total_15_funds_weight_4]
    total_30 = [total_30_funds_weight_0, total_30_funds_weight_1, total_30_funds_weight_2, total_30_funds_weight_3, total_30_funds_weight_4]

    return funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30

def handle(funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30):


    # 1. 当天是靠近月初还是靠近月末
    # 2. switch case

    funds_code = funds_code.applymap(lambda x: str(int(x)).zfill(6) + '.OF')

    return funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30

def upload(funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30):

    funds_code.to_sql('funds_code', con=product_jiayin_robo_advisor, if_exists='append')
    funds_name.to_sql('funds_name', con=product_jiayin_robo_advisor, if_exists='append')
    top_funds_return.to_sql('top_funds_return', con=product_jiayin_robo_advisor, if_exists='append')

    funds_returns[0].to_sql('funds_return_0', con=product_jiayin_robo_advisor, if_exists='append')
    funds_returns[1].to_sql('funds_return_1', con=product_jiayin_robo_advisor, if_exists='append')
    funds_returns[2].to_sql('funds_return_2', con=product_jiayin_robo_advisor, if_exists='append')
    funds_returns[3].to_sql('funds_return_3', con=product_jiayin_robo_advisor, if_exists='append')
    funds_returns[4].to_sql('funds_return_4', con=product_jiayin_robo_advisor, if_exists='append')

    total_9[0].to_sql('total_9_funds_weight_0', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[1].to_sql('total_9_funds_weight_1', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[2].to_sql('total_9_funds_weight_2', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[3].to_sql('total_9_funds_weight_3', con=product_jiayin_robo_advisor, if_exists='append')
    total_9[4].to_sql('total_9_funds_weight_4', con=product_jiayin_robo_advisor, if_exists='append')

    total_10[0].to_sql('total_10_funds_weight_0', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[1].to_sql('total_10_funds_weight_1', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[2].to_sql('total_10_funds_weight_2', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[3].to_sql('total_10_funds_weight_3', con=product_jiayin_robo_advisor, if_exists='append')
    total_10[4].to_sql('total_10_funds_weight_4', con=product_jiayin_robo_advisor, if_exists='append')

    total_15[0].to_sql('total_15_funds_weight_0', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[1].to_sql('total_15_funds_weight_1', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[2].to_sql('total_15_funds_weight_2', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[3].to_sql('total_15_funds_weight_3', con=product_jiayin_robo_advisor, if_exists='append')
    total_15[4].to_sql('total_15_funds_weight_4', con=product_jiayin_robo_advisor, if_exists='append')

    total_30[0].to_sql('total_30_funds_weight_0', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[1].to_sql('total_30_funds_weight_1', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[2].to_sql('total_30_funds_weight_2', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[3].to_sql('total_30_funds_weight_3', con=product_jiayin_robo_advisor, if_exists='append')
    total_30[4].to_sql('total_30_funds_weight_4', con=product_jiayin_robo_advisor, if_exists='append')


def monthly_update():
    print(f'[+] {time.strftime("%c")} start {PRODUCT0}/monthly_update')
    funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30 = read_sql()
    funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30 = handle(funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30)
    upload(funds_code, funds_name, top_funds_return, funds_returns, total_9, total_10, total_15, total_30)
    print(f'[+] {time.strftime("%c")} finish {PRODUCT0}/monthly_update')
    print()
