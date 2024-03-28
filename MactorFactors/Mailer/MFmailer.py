import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import statsmodels.api as sm
import warnings
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pandas.plotting import table
import matplotlib.gridspec as gridspec
import textwrap

from io import BytesIO
import base64
from email.mime.text import MIMEText
from PIL import Image

from settings.database import *
from utils.mysql.get_sql import get_sql
from MactorFactors.Mailer.text import *
from utils.time_function import timeit

warnings.filterwarnings("ignore")


def read_sql(FactorName):

    YoY = get_sql(level3_factors, FactorName + '_YoY')
    WoW = get_sql(level3_factors, FactorName + '_WoW')
    YoY = YoY.set_index('Date')
    WoW = WoW.set_index('Date')
    Factor = pd.merge(YoY, WoW, left_index=True, right_index=True)
    Factor = Factor.resample('B').ffill()
    # Factor = Factor[Factor.index >= '2020-01-01']

    return Factor


def get_idx(idx_name, level=level1_csmar):

    idx = get_sql(level, idx_name)
    idx.rename(columns={'Close':idx_name}, inplace=True)
    idx.set_index('Date', inplace=True)
    idx.index = pd.to_datetime(idx.index)
    
    return idx


def corr_beta(factor, factorname, idx, idx_s, idx_YoY, idx_WoW, rf, inf):
    
    # factor.index = factor.index + pd.Timedelta('5D')
    df1 = pd.merge(factor, idx, left_index=True, right_index=True)
    df1 = pd.merge(df1, idx_YoY, left_index=True, right_index=True)
    df1 = pd.merge(df1, idx_WoW, left_index=True, right_index=True)
    df1 = pd.merge(df1, rf, left_index=True, right_index=True)
    df1[f'{factorname}_YoY_1D'] = df1[f'{factorname}_YoY'].shift(1)
    df1[f'{factorname}_WoW_1D'] = df1[f'{factorname}_WoW'].shift(1)
    df1[f'{factorname}_YoY_7D'] = df1[f'{factorname}_YoY'].shift(5)
    df1[f'{factorname}_WoW_7D'] = df1[f'{factorname}_WoW'].shift(5)
    df1 = df1.dropna()
    df1 = df1[df1.index >= '2020-01-01']
    corr_value = df1[f'{factorname}_YoY'].corr(df1[f'{idx_s}_YoY'])
    inf.loc[idx_s,'同比-corr'] = round(corr_value, 2)
    corr_value2 = df1[f'{factorname}_WoW'].corr(df1[f'{idx_s}_WoW'])
    inf.loc[idx_s,'周环比-corr'] = round(corr_value2, 2)

    X = (df1[f'{idx_s}_YoY']).values.reshape(-1, 1)
    X = sm.add_constant(X)
    y = (df1[f'{factorname}_YoY']).values.reshape(-1, 1)
    model = sm.OLS(y, X)  
    results = model.fit()  
    beta = results.params[1]*100
    t_stat = results.tvalues[1]
    inf.loc[idx_s,'同比-beta(t-stat)'] = f'{beta:.2f}%({t_stat:.2f})'

    X = (df1[f'{idx_s}_YoY']).values.reshape(-1, 1)
    X = sm.add_constant(X)
    y = (df1[f'{factorname}_YoY_1D']).values.reshape(-1, 1)
    model = sm.OLS(y, X)  
    results = model.fit()  
    beta = results.params[1]*100
    t_stat = results.tvalues[1]
    inf.loc[idx_s,'同比-领先一天'] = f'{beta:.2f}%({t_stat:.2f})'

    X = (df1[f'{idx_s}_YoY']).values.reshape(-1, 1)
    X = sm.add_constant(X)
    y = (df1[f'{factorname}_YoY_7D']).values.reshape(-1, 1)
    model = sm.OLS(y, X)  
    results = model.fit()  
    beta = results.params[1]*100
    t_stat = results.tvalues[1]
    inf.loc[idx_s,'同比-领先七天'] = f'{beta:.2f}%({t_stat:.2f})'

    X2 = (df1[f'{idx_s}_WoW']).values.reshape(-1, 1)
    X2 = sm.add_constant(X2)
    y2 = (df1[f'{factorname}_WoW']).values.reshape(-1, 1)
    model2 = sm.OLS(y2, X2)  
    results2 = model2.fit()  
    beta2 = results2.params[1]*100
    t_stat2 = results2.tvalues[1]
    inf.loc[idx_s,'周环比-beta(t-stat)'] = f'{beta2:.2f}%({t_stat2:.2f})'

    X2 = (df1[f'{idx_s}_WoW']).values.reshape(-1, 1)
    X2 = sm.add_constant(X2)
    y2 = (df1[f'{factorname}_WoW_1D']).values.reshape(-1, 1)
    model2 = sm.OLS(y2, X2)  
    results2 = model2.fit()  
    beta2 = results2.params[1]*100
    t_stat2 = results2.tvalues[1]
    inf.loc[idx_s,'周环比-领先一天'] = f'{beta2:.2f}%({t_stat2:.2f})'

    X2 = (df1[f'{idx_s}_WoW']).values.reshape(-1, 1)
    X2 = sm.add_constant(X2)
    y2 = (df1[f'{factorname}_WoW_7D']).values.reshape(-1, 1)
    model2 = sm.OLS(y2, X2)  
    results2 = model2.fit()  
    beta2 = results2.params[1]*100
    t_stat2 = results2.tvalues[1]
    inf.loc[idx_s,'周环比-领先七天'] = f'{beta2:.2f}%({t_stat2:.2f})'

    # percentileWoW = stats.percentileofscore(idx.iloc[:,0].values, idx.iloc[:,0].values[-1], kind='rank')
    # inf.loc[idx_s,'分位数'] = f'{percentileWoW:.2f}'
        
    return inf

    
def handle_corr_beta(factor, factorname, sz, hs300, zz500, zz1000, zz2000, micro, SJN229,sz_YoY, hs300_YoY, zz500_YoY, zz1000_YoY, zz2000_YoY, micro_YoY, SJN229_YoY,sz_WoW, hs300_WoW, zz500_WoW, zz1000_WoW, zz2000_WoW, micro_WoW, SJN229_WoW,rf):    

    inf = pd.DataFrame(columns=['同比-beta(t-stat)','同比-领先一天','同比-领先七天','同比-corr','周环比-beta(t-stat)','周环比-领先一天','周环比-领先七天','周环比-corr'])
    idxs = [sz, hs300, zz500, zz1000, zz2000, micro, SJN229]
    idx_ss = ['sz', 'hs300', 'zz500', 'zz1000', 'zz2000', 'micro', 'SJN229']
    idx_YoYs = [sz_YoY, hs300_YoY, zz500_YoY, zz1000_YoY, zz2000_YoY, micro_YoY, SJN229_YoY]
    idx_WoWs = [sz_WoW, hs300_WoW, zz500_WoW, zz1000_WoW, zz2000_WoW, micro_WoW, SJN229_WoW]
    for i in range(len(idxs)):
        idx = idxs[i]
        idx_s = idx_ss[i]
        idx_YoY = idx_YoYs[i]
        idx_WoW = idx_WoWs[i]
        inf = corr_beta(factor, factorname, idx, idx_s, idx_YoY, idx_WoW, rf, inf)
    inf = inf.reset_index()
    inf.columns = ['指数','同比beta(t-stat)','同比-领先一天','同比-领先一周','同比-corr','周环比beta(t-stat)','周环比-领先一天','周环比-领先一周','周环比-corr']
    inf['指数'] = ['上证指数','沪深300', '中证500', '中证1000', '中证2000', '万得微盘', '水木2号']
    inf = inf.set_index('指数')

    return inf


def image_2(ax1,Factor,ylims,FactorName,FactorName_Chinese):

    Factor = Factor[Factor.index >= '2020-01-01']
    # fig, ax1 = plt.subplots(figsize=(10,6))
    plt.subplots_adjust(bottom=0.5)
    ax1.set_ylim(ylims[0])
    # ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'-{abs(x):.0f}%' if x < 0 else f'{x:.0f}%'))
    ax1.yaxis.tick_left()
    ax2 = ax1.twinx()
    ax2.set_ylim(ylims[1])
    # ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'-{abs(x):.0f}%' if x < 0 else f'{x:.0f}%'))
    ax2.yaxis.tick_right()
    ax1.spines['bottom'].set_visible(False)
    ax1.spines['top'].set_visible(False)
    ax2.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.axhline(0, color='black', linewidth=0.5)
    ax2.axhline(0, color='black', linewidth=0.5)
    line1, = ax1.plot(Factor.index.values, Factor[f'{FactorName}_YoY'].values, color='red', linewidth=1.5)
    line2, = ax2.plot(Factor.index.values, Factor[f'{FactorName}_WoW'].values, color='green', linewidth=0.75)
    plt.title(f'{FactorName_Chinese}', fontsize=16)
    plt.legend([line1, line2], [f'{FactorName_Chinese}同比', f'{FactorName_Chinese}周环比（右轴）'], loc='lower center', bbox_to_anchor=(0.5, -0.35), ncol=2, fontsize=8)
    ax1.grid(True, linestyle='--', linewidth=0.5, color='gray', alpha=0.5,axis='y')


@timeit('MFMailer')
def MFmailer():

    InterestRateFactor = read_sql('InterestRateFactor')
    ExchangeRateFactor = read_sql('ExchangeRateFactor')
    TermSpreadFactor = read_sql('TermSpreadFactor')
    CreditFactor = read_sql('CreditFactor')
    HighFreqEcoGrowth = read_sql('HighFreqEcoGrowth')
    HighFreqInflationConsumer = read_sql('HighFreqInflationConsumer')
    HighFreqInflationProductor = read_sql('HighFreqInflationProductor')

    FactorName = ['InterestRateFactor', 
                    'ExchangeRateFactor',
                    'CreditFactor',
                    'TermSpreadFactor',
                    'HighFreqInflationProductor',
                    'HighFreqInflationConsumer',
                    'HighFreqEcoGrowth'
                    ]
    FactorName_Chinese = ['利率因子',
                            '汇率因子',
                            '信用因子',
                            '期限利差因子',
                            '通货膨胀因子生产端',
                            '通货膨胀因子消费端',
                            '高频经济增长'
                            ]   
    Image_ylims = [[(-10,10),(-5,5)],
                    [(-30,30),(-10,10)],
                    [(-10,10),(-5,5)],
                    [(-10,10),(-5,5)],
                    [(-20,20),(-5,5)],
                    [(-12,12),(-1,1)],
                    [(-15,15),(-5,5)]
                    ]

    sz = get_idx('sz')
    hs300 = get_idx('hs300')
    zz500 = get_idx('zz500')
    zz1000 = get_idx('zz1000')
    zz2000 = get_idx('zz2000',level =level0_wind)
    micro = get_idx('WindMicroCapIdx',level =level0_wind)
    product_fund = get_sql(product_jiayin_robo_advisor, 't_1_fund_networth')
    product_fund.rename(columns={'record_date': 'Date', 'fund_code': 'Private500FundsCode', 'bonus_total_networth': 'AdjNAV'}, inplace=True)
    SJN229 = product_fund[product_fund['Private500FundsCode'] == 'SJN229'][['Date','AdjNAV']]
    SJN229.rename(columns={'AdjNAV':'SJN229'}, inplace=True)
    SJN229['SJN229'] = SJN229['SJN229'].astype('float64')
    SJN229.set_index('Date', inplace=True)

    sz_YoY = sz.pct_change(250)*100
    sz_YoY.rename(columns={'sz':'sz_YoY'}, inplace=True)
    hs300_YoY = hs300.pct_change(250)*100
    hs300_YoY.rename(columns={'hs300':'hs300_YoY'}, inplace=True)
    zz500_YoY = zz500.pct_change(250)*100
    zz500_YoY.rename(columns={'zz500':'zz500_YoY'}, inplace=True)
    zz1000_YoY = zz1000.pct_change(250)*100
    zz1000_YoY.rename(columns={'zz1000':'zz1000_YoY'}, inplace=True)
    zz2000_YoY = zz2000.pct_change(250)*100
    zz2000_YoY.rename(columns={'zz2000':'zz2000_YoY'}, inplace=True)
    micro_YoY = micro.pct_change(250)*100
    micro_YoY.rename(columns={'WindMicroCapIdx':'micro_YoY'}, inplace=True)
    SJN229_YoY = SJN229.pct_change(250)*100
    SJN229_YoY.rename(columns={'SJN229':'SJN229_YoY'}, inplace=True)

    sz_WoW = sz.pct_change(5)*100
    sz_WoW.rename(columns={'sz':'sz_WoW'}, inplace=True)
    hs300_WoW = hs300.pct_change(5)*100
    hs300_WoW.rename(columns={'hs300':'hs300_WoW'}, inplace=True)
    zz500_WoW = zz500.pct_change(5)*100
    zz500_WoW.rename(columns={'zz500':'zz500_WoW'}, inplace=True)
    zz1000_WoW = zz1000.pct_change(5)*100
    zz1000_WoW.rename(columns={'zz1000':'zz1000_WoW'}, inplace=True)
    zz2000_WoW = zz2000.pct_change(5)*100
    zz2000_WoW.rename(columns={'zz2000':'zz2000_WoW'}, inplace=True)
    micro_WoW = micro.pct_change(5)*100
    micro_WoW.rename(columns={'WindMicroCapIdx':'micro_WoW'}, inplace=True)
    SJN229_WoW = SJN229.pct_change(5)*100
    SJN229_WoW.rename(columns={'SJN229':'SJN229_WoW'}, inplace=True)

    rf = get_sql(level0_jiayin, 'm_CH3',index_cols='Date')['rf']*100
    rf.index = pd.to_datetime(rf.index)
    rf = rf.resample('D').fillna(method='ffill')
        
    login_email ='ml_ap_jiayin@163.com'
    password  =  'WLCUYHVLRHFTRAIQ'
    receiver_email = 'market_monitor_hy@163.com'
    subject='Macro Factors'

    msg = MIMEMultipart()
    msg['From'] = login_email
    msg['To'] = receiver_email
    msg['Subject'] = subject 
    images_base64 = []
    bufs = []

    for i in range(len(FactorName)):
        
        inf = handle_corr_beta(eval(FactorName[i]), FactorName[i],sz, hs300, zz500, zz1000, zz2000, micro, SJN229,sz_YoY, hs300_YoY, zz500_YoY, zz1000_YoY, zz2000_YoY, micro_YoY, SJN229_YoY,sz_WoW, hs300_WoW, zz500_WoW, zz1000_WoW, zz2000_WoW, micro_WoW, SJN229_WoW,rf)

        fig = plt.figure(figsize=(8, 4.3))
        gs = gridspec.GridSpec(3, 1, height_ratios=[2.5, 4, 0.5])
        ax0 = plt.subplot(gs[0])
        ax0.axis('off') # 关闭坐标轴
        tbl = table(ax0, inf, loc='center', cellLoc='center', colWidths=[0.11]*len(inf.columns))
        # 设置字体大小
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        # 设置表格大小
        tbl.scale(1.2, 1)
        # 设置表格颜色
        colors = ['#ffffff', '#d3d3d3'] 
        for j, key in enumerate(tbl.get_celld().keys()):
            if key[1] == -1:  # 第一列
                # tbl.get_celld()[key].set_height(0.1)
                tbl.get_celld()[key].set_facecolor('#505050')  # 深灰色
                tbl.get_celld()[key].get_text().set_color('white')  # 字体颜色改为白色
            elif key[0] == 0:  # 表头
                tbl.get_celld()[key].set_height(0.15)
                tbl.get_celld()[key].set_facecolor('#1f77b4')  # 蓝色
                tbl.get_celld()[key].get_text().set_color('white')  # 字体颜色变为白色
                tbl.get_celld()[key].get_text().set_weight('bold')  # 字体加粗
                tbl.get_celld()[key].get_text().set_size(8)  # 字号变为
            else:  # 其他单元格
                tbl.get_celld()[key].set_facecolor(colors[key[0] % 2])  # 使用colors列表循环设置颜色
                # tbl.get_celld()[key].set_height(0.1)
        # 设置表格边框样式
        for key, cell in tbl.get_celld().items():
            cell.set_edgecolor('w')
            cell.set_linewidth(2)

        ax1 = plt.subplot(gs[1])  
        image_2(ax1,Factor=eval(FactorName[i]),ylims=Image_ylims[i],FactorName=FactorName[i],FactorName_Chinese=FactorName_Chinese[i])
        
        ax3 = plt.subplot(gs[2])
        text = eval(f't{i}')
        wrapper = textwrap.TextWrapper(width=70, initial_indent='', subsequent_indent='')
        wrapped_text = wrapper.fill(text)
        ax3.text(0, 0.5, wrapped_text, ha='left', va='top', fontsize=8)
        ax3.axis('off')  # 关闭坐标轴
        
        plt.tight_layout()

        # 将图形保存到缓冲区
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        bufs.append(buf)
        # 将图形转换为 base64 编码，然后添加到列表中
        image_base64 = base64.b64encode(buf.read()).decode()
        images_base64.append(image_base64)

        # buf.close()

    images = [Image.open(buf) for buf in bufs]
    max_width = max(image.width for image in images)
    total_height = sum(image.height for image in images)
    result_image = Image.new('RGB', (max_width, total_height))
    current_y = 0
    for image in images:
        # 由于垂直拼接，每个图像在新图像中的x坐标都是0，y坐标是当前高度
        result_image.paste(image, (0, current_y))
        current_y += image.height
    result_image.save('merged_plot_vertical.png')
    # result_image.show()
    buf = BytesIO()
    result_image.save(buf, format='PNG')
    image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    buf.close()

    for buf in bufs:
        buf.close()
    # html = '<html><body>' + ''.join(f'<img src="data:image/png;base64,{image}" alt="image"><br>' for image in images_base64) + '</body></html>'
    html = f'<html><body><img src="data:image/png;base64,{image_base64}" alt="image"></body></html>'
    # 创建一个 MIMEText 对象，然后将它添加到邮件中
    msg.attach(MIMEText(html, 'html'))


        # buf = BytesIO()
        # plt.savefig(buf, format='png')
        # buf.seek(0)
        # img = MIMEImage(buf.read())
        # msg.attach(img)
        # buf.close()

    try:
        # 创建SMTP连接
        server = smtplib.SMTP('smtp.163.com', 25)  # 163邮箱SMTP服务器地址和端口
        server.login(login_email, password)  # 登录邮箱
        server.send_message(msg)  # 发送邮件
        server.quit()  # 关闭连接
        print("邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {e}")


if __name__ == '__main__':
    MFmailer()
