import pymysql
import time
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.time_function import timeit

from MactorFactors.Mailer.MFmailer import MFmailer

def get_tradingdata_from_mysql(database, sql_str,host_ip = '10.8.3.37',
                               port_num = 33306,
                               user_name = 'xxx',
                               password = 'xxx'):

    '''
    This function is used to get dataframe from mysql database

    :param 
    sql_str: sql likc code such as "SELECT * FROM ASHARECALENDAR"
    database: sql database such as：clearingsystem
    
    :return: a pandas DataFrame which get from mysql database

    '''

    # 0 load data from sql database
    # 打开数据库连接（请根据自己的用户名、密码及数据库名称进行修改）

    conn = pymysql.connect(host=host_ip,
                           port=port_num,
                           user=user_name,
                           password=password,
                           database=database,
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)

    cursor = conn.cursor()
    time_start = time.time()
    cursor.execute(sql_str)
    values = cursor.fetchall()
    table_file = pd.DataFrame(values)
    time_end = time.time()  # 结束计时
    time_c = time_end - time_start  # 运行所花时间
    print('从底层数据库取数据耗费时间为', time_c, 's')

    return table_file


def send_email_with_dataframe(login_email, password, receiver_email, subject, dataframe):
    """
    使用163邮箱发送包含DataFrame的电子邮件

    参数:
    - login_email: 登录邮箱
    - password: !!不是密码!! 是SMTP授权码: xxx
    - receiver_email: 接收者邮箱
    - subject: 邮件主题
    - dataframe: 要发送的DataFrame
    """
    # 将DataFrame转换为HTML
    html = dataframe.to_html()

    # 创建MIME多部分消息对象，并设置邮件头部信息
    msg = MIMEMultipart()
    msg['From'] = login_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # 将HTML字符串附加到邮件正文
    msg.attach(MIMEText(html, 'html'))

    try:
        # 创建SMTP连接
        server = smtplib.SMTP('smtp.163.com', 25)  # 163邮箱SMTP服务器地址和端口
        server.login(login_email, password)  # 登录邮箱
        server.send_message(msg)  # 发送邮件
        server.quit()  # 关闭连接
        print("邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {e}")

@timeit('StockMailer')
def handle():
    all_holding = get_tradingdata_from_mysql('public_database', "SELECT * FROM daily_nn",
                                         host_ip = '10.8.3.37', port_num = 33306)
    all_holding = all_holding.drop(all_holding.columns[-1], axis=1)
    all_holding.rename(columns={'TRADE_DT': '时间', 
                                'name': '股票名称', 
                                'score': '未来2周预期收益率',
                                'S_INFO_WINDCODE':'股票代码'}, inplace=True)
    all_holding = all_holding[all_holding['时间'] == all_holding['时间'].max()].reset_index(drop=True)
    send_email_with_dataframe('xxx', 'xxx', 
                              'xxx', subject='Stock', dataframe=all_holding)


if __name__ == '__main__':
    handle()
    MFmailer()

