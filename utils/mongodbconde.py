import pymongo
import datetime
import pandas as pd

def fetch_data(start_date, end_date, collection, time_query_key='TRADE_DT', factor_ls=None):
    """
    从数据库中读取需要指定日期范围的数据,包含startdate，包含enddate

    start_date:which date your need factors, str,'1991-01-01'
    end_date:str,'1991-01-01'
    collection: select collection after connecting to mongodb, such as: 'TRADE_DT'
    time_query_key: str, time key name for query database
    save_list: list with variable your need, make sure your variable is right,
                default= 'all',get all data

    比如，当我需要从Mongodb数据库中factor数据中获取factor这个collection，需要按照以下命令：
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.factor
    collection = db.factor

    注意 TODO：目前function不能一次取超过3年的数据，否则内存要爆，要取全部年份，需要写循环
    """
    if end_date is not None:
        # 将end-date延后一天，以便形成闭区间
        end_date = (pd.to_datetime(end_date) + pd.Timedelta(1, unit='d')).strftime('%Y-%m-%d')
        query = {time_query_key: {"$gte": start_date, "$lte": end_date}}
    else:
        query = {time_query_key: {'$gte': start_date}}

    print('Querying......')

    if factor_ls is not None:
        fields = dict.fromkeys(factor_ls, 1)
        cursor = collection.find(query, fields)
    else:
        cursor = collection.find(query)
    data = pd.DataFrame.from_records(cursor)

    if len(data) != 0:
        data[time_query_key] = pd.to_datetime(data[time_query_key])
    return data


# client = pymongo.MongoClient('mongodb://my_tester:123456@10.8.3.37:27017/')
# collection = client['basic_data']['Daily_return_with_cap']
# data_return_raw = fetch_data('2023-01-01', str(datetime.datetime.today())[:10], collection,
#                                      time_query_key='TRADE_DT')


