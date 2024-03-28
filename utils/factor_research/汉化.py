

def 汉化(df):
    df = df.rename(columns=columns_mapper)
    df = value_mapper(df)
    return df


第1级 = '活钱管理'
第2级 = '稳健理财'
第3级 = '跑赢通胀'
第4级 = '追求增值'
第5级 = '追求高收益'


columns_mapper = {'interval_start': '开始时间',
'interval_end': '结束时间',
'category': '类别',
'mean_monthly_return': '月平均收益',
'base_mean_monthly_return': '基准月平均收益',
'std_mean_return': '月收益标准差',
'base_std_monthly_return': '基准月收益标准差',
'interval_length': '持续时间',
'positive_excess_return_months': '超额收益>=0',
'positive_excess_return_months_proportion': '占比',
'most_conservative': 第1级,
'moderate_conservative': 第2级,
'balanced': 第3级,
'moderate_risky': 第4级,
'most_risky': 第5级,
'last_n_month': '过去N月',
'statistics': '统计量',
'return': '收益率',
'excess return': '超额收益率',
'annual return': '年化收益率',
'annual excess return': '年化超额收益率',
'last 1 month': '过去1月',
'last 3 month': '过去3月',
'last 6 month': '过去6月',
'last 12 month': '过去12月',
'last 36 month': '过去36月',
'last 1 month': '过去1月',
'hs300': '沪深300',
'type': '类别',
'last year return': '过去一年回报',
'mdd 3 month during last year': '过去一年中每3个月最大回撤',
'market_status': '市场状态',
'advanced_statistics': '进阶统计量',
'annual_sharpe': '年化夏普比率',
'annual_volatility': '年化波动率',
'annual_return': '年化收益率',
'β': '贝塔',
'past_year_mdd': '过去一年连续3个月最大回撤',
'mdd': '连续3个月最大回撤'
}


def value_mapper(df):
    df = df.replace('most_conservative', 第1级)
    df = df.replace('moderate_conservative', 第2级)
    df = df.replace('balanced', 第3级)
    df = df.replace('moderate_risky', 第4级)
    df = df.replace('most_risky', 第5级)

    for time in [1, 3, 6, 12 ,36]:
        df = df.replace(f'last {time} month', f'过去{time}月')


    df = df.replace('return', '收益率')
    df = df.replace('excess return', '超额收益率')
    df = df.replace('annual return', '年化收益率')
    df = df.replace('annual excess return', '年化超额收益率')

    df = df.replace('bull', '牛市')
    df = df.replace('bear', '熊市')

    df = df.replace('hs300', '沪深300')
        
    return df

    