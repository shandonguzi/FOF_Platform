'''存量数据入库debug时使用'''
import time
import MySQLdb

def del_sql(con, table_name):

    db = MySQLdb.connect(host=con.url.host, user=con.url.username, password=con.url.password, database=con.url.database, port=con.url.port, charset='utf8')
    cursor = db.cursor()
    query = f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    if result == 1:
        cursor.execute(f'DROP TABLE {table_name}')
        print(f'[-] {time.strftime("%c")} {table_name} deleted')
    else:
        print(f'[=] {time.strftime("%c")} {table_name} does not exist')
    db.commit()

 