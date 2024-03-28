
import numpy as np
import pandas as pd
from dask import delayed

def p_to_sql(dataframe, table_name: str, con : str, index : list=False, partitions : int=1, n_workers : int=1, threads_per_worker : int=1, **to_sql_kargs) -> None:

    dataframe.iloc[:0].to_sql(table_name, con, if_exists='append', index=False)
    dataframe = dataframe.replace(np.nan, 'NULL')
    all_parts = np.array_split(dataframe, partitions)
    import MySQLdb
    def partly_insert(part_sequence):
        part= all_parts[part_sequence]
        engine = MySQLdb.connect(host=con.url.host, user=con.url.username, password=con.url.password, database=con.url.database, port=con.url.port, charset='utf8')
        cursor = engine.cursor()
        insert_sql = f"insert into\
    {table_name}(`{'`, `'.join(part.columns.values)}`)\
    values\
        {str(list(part.itertuples(index=False, name=None))).strip('[').strip(']')}".replace("'NULL'", 'NULL')

        cursor.execute(insert_sql)
        engine.commit()
        engine.close()
        return 0

    results = []
    for part_sequence in np.arange(partitions):
        results.append(delayed(partly_insert)(part_sequence))

    if type(dataframe) != pd.Series:
        delayed(sum)(results).compute(n_workers=n_workers, threads_per_worker=threads_per_worker)
    else:
        series = dataframe
        series.to_sql(table_name, con, **to_sql_kargs)

    if index:
        engine = MySQLdb.connect(host=con.url.host, user=con.url.username, password=con.url.password, database=con.url.database, port=con.url.port, charset='utf8')
        cursor = engine.cursor()
        try :
            cursor.execute(f'ALTER TABLE `{table_name}` ADD INDEX ({", ".join(index)})')
        except:
            pass
        engine.close()

pd.DataFrame.p_to_sql = p_to_sql
