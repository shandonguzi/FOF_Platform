
import MySQLdb
import sqlalchemy.engine


def sql_exec(con : sqlalchemy.engine, sql : str) -> None:
    '''
    直接执行sql语句
    con: sqlalchemy.engine
    sql: str
    return: None
    '''
    
    engine = MySQLdb.connect(host=con.url.host, user=con.url.username, password=con.url.password, database=con.url.database, port=con.url.port, charset='utf8')
    cursor = engine.cursor()
    cursor.execute(sql)
    engine.commit()
    engine.close()
