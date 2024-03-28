import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from dateutil.relativedelta import relativedelta
import statsmodels.api as sm
import warnings
import matplotlib.ticker as mtick

warnings.filterwarnings('ignore')


def read_sql(name):
    df = pd.read_excel('/code/MactorFactors/newtow/data/{}.xlsx'.format(name))
    return df


def handle_df(df,factor):
    df = df.dropna()
    df.columns = ['Date', str(factor)]
    df = df.set_index('Date')
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    if str(factor)=='PPI':
        df = df-100
    # if str(factor) == 'TotalRetailSales' or str(factor) == 'IndustrialAddedValueIndex':
    #     df.index = df.index + pd.offsets.MonthEnd(1)
    return df


def timechoose(df, start, end=None):
    df = df[start:end]
    df = df[(df.index >= start) & (df.index <= end) if end else (df.index >= start)]
    return df


# 填补缺失值
def fill_missing_values(column):
    # 根据过去12个月的一阶差分中位数来填充列中的缺失值。
    diff_series = column.diff()
    diff_median12 = diff_series.shift(1).rolling(window=12, min_periods=1).median()
    fill_values = column.copy()
    missing_indexes = column[column.isna()].index
    if not missing_indexes.empty:
        if missing_indexes[0] == column.index[0]:
            missing_indexes = missing_indexes.drop(missing_indexes[0])
            for idx in missing_indexes:
                # 首先判断前面有没有非nan值，如果没有，就在missing_indexes中删除该值，继续对下一个值进行判断
                idx_pos = column.index.get_loc(idx)
                if pd.isna(column.iloc[idx_pos - 1]):
                    missing_indexes = missing_indexes.drop(idx)
                    continue
                else:
                    break
        for idx in missing_indexes:
            idx_pos = column.index.get_loc(idx)
            fill_values[idx] = (fill_values.iloc[idx_pos - 1] + diff_median12.loc[idx])
            # fill_values[idx] = fill_values.iloc[idx_pos - 1] if pd.isna(column.iloc[idx_pos - 1]) else column.iloc[idx_pos - 1] + diff_median12.loc[idx]

    return column.fillna(fill_values)


# 基于同比数据调整绝对值
def adjust_absolute_value(column, columnYoY, base_year):    

    adjusted_values = pd.Series(index=column.index, dtype=float)
    for month in column[column.index.year == base_year].index:
        adjusted_values[month] = column[month]   
    # 处理基年之后的数据（向后调整）
    for year in range(base_year + 1, column.index.year.max() + 1):
        for month in column[column.index.year == year].index.month:
            # 计算新的绝对值
            prev_year_value = adjusted_values.loc[pd.Timestamp(year=year-1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = columnYoY.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            adjusted_values.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = prev_year_value * (1 + yoy_growth)
    # 处理基年之前的数据（向前调整）
    for year in range(base_year - 1, column.index.year.min() - 1, -1):
        for month in column[column.index.year == year].index.month:
            # 计算新的绝对值
            next_year_value = adjusted_values.loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = columnYoY.loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            adjusted_values.loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = next_year_value / (1 + yoy_growth)

    return adjusted_values


def handle_adjdata(all_factors_filled):
    # 对于TotalRetailSales，使用2011年各月的当月值作为基年，使用同比值向前和向后进行调整
    # if 'TotalRetailSales' not in all_factors_filled.columns:
    #     raise Exception("Column 'TotalRetailSales' does not exist in the DataFrame.")
    all_factors_filled['AdjTotalRetailSales'] = adjust_absolute_value(all_factors_filled['TotalRetailSales'], all_factors_filled['TotalRetailSalesYoY'], 2011)
    # CPI 定基指数和 PPI 定基指数，也做同样处理
    all_factors_filled['AdjCPI'] = pd.Series(index=all_factors_filled.index, dtype=float)
    all_factors_filled['AdjCPI'] = all_factors_filled['AdjCPI'].fillna(1)
    all_factors_filled['AdjCPI'] = adjust_absolute_value(all_factors_filled['AdjCPI'], all_factors_filled['CPI'], 2011)

    all_factors_filled['AdjPPI'] = pd.Series(index=all_factors_filled.index, dtype=float)
    all_factors_filled['AdjPPI'] = all_factors_filled['AdjPPI'].fillna(1)
    all_factors_filled['AdjPPI'] = adjust_absolute_value(all_factors_filled['AdjPPI'], all_factors_filled['PPI'], 2011)
    # 用IndustrialAddedValueIndexYoY填补IndustrialAddedValueIndex的缺失值
    first_valid_index = all_factors_filled['IndustrialAddedValueIndex'].first_valid_index()
    for year in range(first_valid_index.year - 1, all_factors_filled.index.year.min() - 1, -1):
        for month in all_factors_filled[all_factors_filled.index.year == year].index.month:
            next_year_value = all_factors_filled['IndustrialAddedValueIndex'].loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)]
            yoy_growth = all_factors_filled['IndustrialAddedValueIndexYoY'].loc[pd.Timestamp(year=year+1, month=month, day=1) + pd.offsets.MonthEnd(1)] / 100
            all_factors_filled['IndustrialAddedValueIndex'].loc[pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)] = next_year_value / (1 + yoy_growth)

    return all_factors_filled


def seasadj(all_factors_filled):

    all_factors_seasadj = pd.DataFrame()
    all_factors_seasadj['IndustrialAddedValueIndex'] = sm.tsa.x13_arima_analysis(all_factors_filled['IndustrialAddedValueIndex'],
                                                                                trading=True,
                                                                                x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['PMI'] = sm.tsa.x13_arima_analysis(all_factors_filled['PMI'],
                                                        trading=True,
                                                        x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['AdjTotalRetailSales'] = sm.tsa.x13_arima_analysis(all_factors_filled['AdjTotalRetailSales'],
                                                                        trading=True,
                                                                        x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['AdjCPI'] = sm.tsa.x13_arima_analysis(all_factors_filled['AdjCPI'],
                                                            trading=True,
                                                            x12path="/code/x13as/x13as_ascii").seasadj
    all_factors_seasadj['AdjPPI'] = sm.tsa.x13_arima_analysis(all_factors_filled['AdjPPI'],
                                                            trading=True,
                                                            x12path="/code/x13as/x13as_ascii").seasadj

    return all_factors_seasadj


def cal_EcoGrowth(all_factors_hp): # 合成经济增长同比:将工业增加值同比、PMI同比差分、社会消费品零售总额同比以滚动24个月波动率倒数加权的方式合成经济增长同比

    all_factors_hp['24Volatility_IndustrialAddedValueIndex'] = all_factors_hp['IndustrialAddedValueIndex'].rolling(window=24).std()
    all_factors_hp['24Volatility_PMI'] = all_factors_hp['PMI'].rolling(window=24).std()
    all_factors_hp['24Volatility_TotalRetailSales'] = all_factors_hp['AdjTotalRetailSales'].rolling(window=24).std() 
    all_factors_hp['Weight_IndustrialAddedValueIndex'] = 1 / (all_factors_hp['24Volatility_IndustrialAddedValueIndex']) 
    all_factors_hp['Weight_PMI'] = 1 / (all_factors_hp['24Volatility_PMI'])
    all_factors_hp['Weight_TotalRetailSales'] = 1 / (all_factors_hp['24Volatility_TotalRetailSales'])
    all_factors_hp['denom'] = all_factors_hp['Weight_IndustrialAddedValueIndex'] + all_factors_hp['Weight_PMI'] + all_factors_hp['Weight_TotalRetailSales']
    all_factors_hp['EcoGrowth'] = all_factors_hp['IndustrialAddedValueIndex'] * all_factors_hp['Weight_IndustrialAddedValueIndex'] / all_factors_hp['denom'] + \
                                all_factors_hp['PMI'] * all_factors_hp['Weight_PMI'] / all_factors_hp['denom'] + \
                                all_factors_hp['AdjTotalRetailSales'] * all_factors_hp['Weight_TotalRetailSales'] / all_factors_hp['denom']

    return all_factors_hp['EcoGrowth']


def get_lowfreq():

    factors_names = {'IndustrialAddedValueIndex':'中国_规模以上工业增加值_定基指数',
                    'PMI':'中国_制造业PMI',
                    'TotalRetailSales':'中国_社会消费品零售总额_当月值',
                    'CPI':'中国_CPI_当月同比',
                    'PPI':'中国_PPI_全部工业品_当月同比',
                    'TotalRetailSalesYoY':'中国_社会消费品零售总额_当月同比',
                    'IndustrialAddedValueIndexYoY':'中国_规模以上工业增加值_当月同比'}
    # 读取数据、预处理
    all_factors = pd.DataFrame()
    for factor in factors_names.keys():
        factor_df = read_sql(factors_names[factor])
        factor_df = handle_df(factor_df, factor)
        factor_df = timechoose(factor_df, '2006-07-01', '2023-09-30')
        all_factors = pd.concat([all_factors, factor_df], axis=1)
    # 填补缺失值
    all_factors_filled = all_factors.apply(fill_missing_values)
    # 根据YoY调整
    all_factors_filled = handle_adjdata(all_factors_filled)
    # 季节性调整
    all_factors_seasadj = seasadj(all_factors_filled)
    # 计算同比值,对于PMI 则计算同比差分
    all_factors_seasadj['IndustrialAddedValueIndexYoY'] = all_factors_seasadj['IndustrialAddedValueIndex'].pct_change(periods=12) * 100
    all_factors_seasadj['PMIYoY'] = all_factors_seasadj['PMI'].diff(periods=12)
    all_factors_seasadj['AdjTotalRetailSalesYoY'] = all_factors_seasadj['AdjTotalRetailSales'].pct_change(periods=12) * 100
    all_factors_seasadj['AdjCPIYoY'] = all_factors_seasadj['AdjCPI'].pct_change(periods=12) * 100
    all_factors_seasadj['AdjPPIYoY'] = all_factors_seasadj['AdjPPI'].pct_change(periods=12) * 100
    # 单向 hp 滤波处理
    all_factors_hp = pd.DataFrame()
    all_factors_hp['IndustrialAddedValueIndex'] = sm.tsa.filters.hpfilter(all_factors_seasadj['IndustrialAddedValueIndexYoY'].dropna(), lamb=1)[1]
    all_factors_hp['PMI'] = sm.tsa.filters.hpfilter(all_factors_seasadj['PMIYoY'].dropna(), lamb=1)[1]
    all_factors_hp['AdjTotalRetailSales'] = sm.tsa.filters.hpfilter(all_factors_seasadj['AdjTotalRetailSalesYoY'].dropna(), lamb=1)[1]
    all_factors_hp['AdjCPI'] = sm.tsa.filters.hpfilter(all_factors_seasadj['AdjCPIYoY'].dropna(), lamb=1)[1]
    all_factors_hp['AdjPPI'] = sm.tsa.filters.hpfilter(all_factors_seasadj['AdjPPIYoY'].dropna(), lamb=1)[1]
    # 数据偏移
    all_factors_hp['IndustrialAddedValueIndex'] = all_factors_hp['IndustrialAddedValueIndex'].shift(1)
    all_factors_hp['AdjTotalRetailSales'] = all_factors_hp['AdjTotalRetailSales'].shift(1)
    all_factors_hp = all_factors_hp.dropna()
    # 低频指标
    EcoGrowth = cal_EcoGrowth(all_factors_hp)
    InflationConsumer = all_factors_hp['AdjCPI']
    InflationProducer = all_factors_hp['AdjPPI']
    # InflationConsumer.index = InflationConsumer.index + pd.offsets.MonthEnd(1)
    # InflationProducer.index = InflationProducer.index + pd.offsets.MonthEnd(1)


    return EcoGrowth, InflationConsumer, InflationProducer



# (3)计算资产权重:以领先期 n 带入回归模型，第 i 月月底以 i-24 个月到第 i 月的低频宏观因子为因变量，
# i-24-n 到 i-n 的资产同比收益率为自变量，构建多元领先回归模型，确定第 i+1 月用于合成高频宏观因子的资产权重。
def get_weights(n, y, *args):
    y=y.dropna()
    args = [x.dropna() for x in args]
    # 确保x和y的起始日期相同
    start_date = max([x.index[0] for x in args] + [y.index[0]])
    y = y[y.index >= start_date]
    args = [x[x.index >= start_date] for x in args]
    weights = []
    shifted_series = [x for x in args]
    totalX = pd.concat(shifted_series, axis=1)
    for i in range(n+24+1, len(y)):
        x_ = totalX[(i-24-n-1):(i-n)].values
        y_ = y[i-24-1:i].values
        x_ = sm.add_constant(x_)
        model = sm.OLS(y_, x_).fit()
        weights.append(model.params[1:])
    weights = pd.DataFrame(weights, columns=totalX.columns)
    weights.columns = 'weight_' + weights.columns
    weights.index = y.index[24+n+1:]
    return weights


def get_highfreq(EcoGrowth, InflationConsumer, InflationProducer):

    factors_names = {'HSI':'highfreq_created/恒生指数',
                    'CRBSpotMetal':'highfreq_created/CRB现货指数_金属',
                    'AgriPriceIdx':'highfreq_created/食用农产品价格指数',
                    'PorkPrice':'highfreq_created/中国_大宗价_猪肉',
                    'ProdMatIdx':'highfreq_created/中国_生产资料价格指数',
                    'CRBSpotIndMat':'highfreq_created/CRB现货指数_工业原料',
                    'CRBSpotComposite':'highfreq_created/CRB现货指数_综合',
                    }
    # 读取数据、预处理
    factor_all_df = pd.DataFrame()
    # factors_YoYRet = pd.DataFrame()
    # factors_WoWRet = pd.DataFrame()
    for factor in factors_names.keys():
        factor_df = read_sql(factors_names[factor])
        factor_df = handle_df(factor_df,factor)
        factor_df = timechoose(factor_df, '2007-09-01', '2023-09-30')
        factor_all_df = pd.concat([factor_all_df, factor_df], axis=1)
    #     factor_YoYRet = factor_df.resample('M').last().pct_change(12).dropna()
    #     factors_YoYRet = pd.concat([factors_YoYRet, factor_YoYRet], axis=1)
    #     factor_WoWRet = factor_df.resample('W').last().pct_change(52).dropna()
    #     factors_WoWRet = pd.concat([factors_WoWRet, factor_WoWRet], axis=1)
        
    factor_all_df = factor_all_df.apply(fill_missing_values)
    factor_all_df = factor_all_df.fillna(method='ffill')
    
    factors_YoYRet_M = factor_all_df.resample('M').last().pct_change(12).dropna()*100
    factors_YoYRet_W = factor_all_df.resample('W').last().pct_change(52).dropna()*100
    factors_WoWRet_W = factor_all_df.resample('W').last().pct_change(1).dropna()*100


    x11 = factors_YoYRet_M['HSI']
    x12 = factors_YoYRet_M['CRBSpotMetal']
    x21 = factors_YoYRet_M['AgriPriceIdx']
    x22 = factors_YoYRet_M['PorkPrice']
    x31 = factors_YoYRet_M['ProdMatIdx']
    x32 = factors_YoYRet_M['CRBSpotIndMat']
    x33 = factors_YoYRet_M['CRBSpotComposite']
    y1 = EcoGrowth
    y2 = InflationConsumer
    y3 = InflationProducer

    weights_EcoGrowth = get_weights(1, y1, x11, x12)
    weights_InfConsumer = get_weights(2, y2, x21, x22)
    weights3_InfProductor = get_weights(4, y3, x31, x32, x33)

    weights_EcoGrowth_week = weights_EcoGrowth.resample('W').ffill()
    weights_InfConsumer_week = weights_InfConsumer.resample('W').ffill()
    weights_InfProductor_week = weights3_InfProductor.resample('W').ffill()

    # def weightvalue(weights1week, weights1):
    #     weights1week2 = weights1week.copy()
    #     weights1week2.index = weights1week2.index.to_period('M')
    #     weights12 = weights1.copy()
    #     weights12.index = weights12.index.to_period('M')
    #     weights1week2.update(weights12)
    #     weights1week2.index = weights1week.index
    #     weights1week = weights1week2
    #     return weights1week

    # weights_EcoGrowth_week = weightvalue(weights_EcoGrowth_week,weights_EcoGrowth)
    # weights_InfConsumer_week = weightvalue(weights_InfConsumer_week,weights_InfConsumer)
    # weights_InfProductor_week = weightvalue(weights_InfProductor_week,weights3_InfProductor)


    df_combined1 = pd.concat([factors_YoYRet_W['HSI'], factors_YoYRet_W['CRBSpotMetal'], weights_EcoGrowth_week], axis=1, join='inner')
    df_combined2 = pd.concat([factors_YoYRet_W['AgriPriceIdx'], factors_YoYRet_W['PorkPrice'], weights_InfConsumer_week], axis=1, join='inner')
    df_combined3 = pd.concat([factors_YoYRet_W['ProdMatIdx'], factors_YoYRet_W['CRBSpotIndMat'], factors_YoYRet_W['CRBSpotComposite'], weights_InfProductor_week], axis=1, join='inner')

    HighFreqEcoGrowthYoY = df_combined1['weight_HSI'] * df_combined1['HSI'] + df_combined1['weight_CRBSpotMetal'] * df_combined1['CRBSpotMetal'] 
    HighFreqEcoGrowthYoY.name = 'HighFreqEcoGrowthYoY'
    HighFreqInflationConsumerYoY = df_combined2['weight_AgriPriceIdx'] * df_combined2['AgriPriceIdx'] + df_combined2['weight_PorkPrice'] * df_combined2['PorkPrice']
    HighFreqInflationConsumerYoY.name = 'HighFreqInflationConsumerYoY'
    HighFreqInflationProducerYoY = df_combined3['weight_ProdMatIdx'] * df_combined3['ProdMatIdx'] + df_combined3['weight_CRBSpotIndMat'] * df_combined3['CRBSpotIndMat'] + df_combined3['weight_CRBSpotComposite'] * df_combined3['CRBSpotComposite']
    HighFreqInflationProducerYoY.name = 'HighFreqInflationProducerYoY'

    df_combinedWoW1 = pd.concat([factors_WoWRet_W['HSI'], factors_WoWRet_W['CRBSpotMetal'], weights_EcoGrowth_week], axis=1, join='inner')
    df_combinedWoW2 = pd.concat([factors_WoWRet_W['AgriPriceIdx'], factors_WoWRet_W['PorkPrice'], weights_InfConsumer_week], axis=1, join='inner')
    df_combinedWoW3 = pd.concat([factors_WoWRet_W['ProdMatIdx'], factors_WoWRet_W['CRBSpotIndMat'], factors_WoWRet_W['CRBSpotComposite'], weights_InfProductor_week], axis=1, join='inner')
    
    HighFreqEcoGrowthWoW = df_combinedWoW1['weight_HSI'] * df_combinedWoW1['HSI'] + df_combinedWoW1['weight_CRBSpotMetal'] * df_combinedWoW1['CRBSpotMetal']
    HighFreqEcoGrowthWoW.name = 'HighFreqEcoGrowthWoW'
    HighFreqInflationConsumerWoW = df_combinedWoW2['weight_AgriPriceIdx'] * df_combinedWoW2['AgriPriceIdx'] + df_combinedWoW2['weight_PorkPrice'] * df_combinedWoW2['PorkPrice']
    HighFreqInflationConsumerWoW.name = 'HighFreqInflationConsumerWoW'
    HighFreqInflationProducerWoW = df_combinedWoW3['weight_ProdMatIdx'] * df_combinedWoW3['ProdMatIdx'] + df_combinedWoW3['weight_CRBSpotIndMat'] * df_combinedWoW3['CRBSpotIndMat'] + df_combinedWoW3['weight_CRBSpotComposite'] * df_combinedWoW3['CRBSpotComposite']
    HighFreqInflationProducerWoW.name = 'HighFreqInflationProducerWoW'

    HighFreqEcoGrowth = pd.concat([HighFreqEcoGrowthYoY, HighFreqEcoGrowthWoW], axis=1, join='inner')
    HighFreqInflationConsumer = pd.concat([HighFreqInflationConsumerYoY, HighFreqInflationConsumerWoW], axis=1, join='inner')
    HighFreqInflationProducer = pd.concat([HighFreqInflationProducerYoY, HighFreqInflationProducerWoW], axis=1, join='inner')

    return HighFreqEcoGrowth, HighFreqInflationConsumer, HighFreqInflationProducer

def image(df,df2=None):
    fig, ax1 = plt.subplots(figsize=(10,6))
    ax1.yaxis.tick_left()
    ax1.spines['bottom'].set_visible(False)
    ax1.spines['top'].set_visible(False)
    ax1.axhline(0, color='black', linewidth=0.5)
    ax1.plot(df.index.values, df.values, color='red', linewidth=2)
    plt.title(str(df.name))
    plt.savefig(f'/code/MactorFactors/create_highfreq/{str(df.name)}.png')
    print(f'image-{str(df.name)}-completed')
    
def create_image(Factor, FactorName):

    fig, ax1 = plt.subplots(figsize=(10,6))
    # ax1.set_ylim(-10, 10)
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.yaxis.tick_left()
    ax2 = ax1.twinx()
    # ax2.set_ylim(-5, 5)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax2.yaxis.tick_right()
    ax1.spines['bottom'].set_visible(False)
    ax1.spines['top'].set_visible(False)
    ax2.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.axhline(0, color='black', linewidth=0.5)
    ax2.axhline(0, color='black', linewidth=0.5)
    ax1.plot(Factor.index.values, Factor[f'{FactorName}YoY'].values, color='red', linewidth=2)
    ax2.plot(Factor.index.values, Factor[f'{FactorName}WoW'].values, color='green', linewidth=1)
    plt.title(f'Image_{FactorName}')
    plt.savefig(f'/code/MactorFactors/create_highfreq/Image_{FactorName}.png')
    # plt.show()
    print(f'image-{str(FactorName)}-completed')


def handle_highfreq():
    EcoGrowth, InflationConsumer, InflationProducer = get_lowfreq()
    HighFreqEcoGrowth, HighFreqInflationConsumer, HighFreqInflationProducer = get_highfreq(EcoGrowth, InflationConsumer, InflationProducer)
    create_image(HighFreqEcoGrowth, 'HighFreqEcoGrowth')
    create_image(HighFreqInflationConsumer, 'HighFreqInflationConsumer')
    create_image(HighFreqInflationProducer, 'HighFreqInflationProducer')
    return HighFreqEcoGrowth, HighFreqInflationConsumer, HighFreqInflationProducer
    
    
if __name__ == '__main__':
    HighFreqEcoGrowth, HighFreqInflationConsumer, HighFreqInflationProducer = handle_highfreq() 

    
    print(HighFreqEcoGrowth, HighFreqInflationConsumer, HighFreqInflationProducer)



