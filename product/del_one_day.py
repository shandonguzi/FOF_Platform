
from settings.database import product_jiayin_robo_advisor
from utils.mysql.get_sql import get_sql

def delete_daily(date):

    get_sql(product_jiayin_robo_advisor, 'advise_on_entry', f'delete from advise_on_entry where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'advise_on_rebalance', f'delete from advise_on_rebalance where Date="{date}"')

    get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_2_rebalance', f'delete from total_9_funds_weight_2_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_3_rebalance', f'delete from total_9_funds_weight_3_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_9_funds_weight_4_rebalance', f'delete from total_9_funds_weight_4_rebalance where Date="{date}"')

    get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_2_rebalance', f'delete from total_10_funds_weight_2_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_3_rebalance', f'delete from total_10_funds_weight_3_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_10_funds_weight_4_rebalance', f'delete from total_10_funds_weight_4_rebalance where Date="{date}"')

    get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_2_rebalance', f'delete from total_15_funds_weight_2_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_3_rebalance', f'delete from total_15_funds_weight_3_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_15_funds_weight_4_rebalance', f'delete from total_15_funds_weight_4_rebalance where Date="{date}"')

    get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_2_rebalance', f'delete from total_30_funds_weight_2_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_3_rebalance', f'delete from total_30_funds_weight_3_rebalance where Date="{date}"')
    get_sql(product_jiayin_robo_advisor, 'total_30_funds_weight_4_rebalance', f'delete from total_30_funds_weight_4_rebalance where Date="{date}"')

