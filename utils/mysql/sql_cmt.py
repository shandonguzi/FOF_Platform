
import MySQLdb
import sqlalchemy

def dict_to_sql(comments: dict):
    '''
    将包含 `{"列名": "注释"}` 的字典转换为sql语句，支持多个列名同时传入
    comments: dict
    return: str
    '''
    statements = []
    for key, value in comments.items():
        statement = f'modify column `{key}` double comment "{value}"'
        statements.append(statement)
    return ", ".join(statements)

def sql_cmt(con: sqlalchemy.engine, table_name: str, comments_dict: dict):
    '''
    对 `con` 中的 `table_name` 表的列名进行如 `comments_dict` 映射的注释
    con: sqlalchemy.engine
    table_name: str
    comments_dict: dict
    return: None
    '''
    engine = MySQLdb.connect(host=con.url.host, user=con.url.username, password=con.url.password, database=con.url.database, port=con.url.port, charset='utf8')
    cursor = engine.cursor()
    comment = f"alter table `{table_name}` {dict_to_sql(comments_dict)};"
    comment
    cursor.execute(comment)
    engine.commit()
    engine.close()
