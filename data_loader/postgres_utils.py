import pandas as pd
import psycopg2 as pg
import psycopg2.extras as pg_extras
from numpy import nan as np_nan


conn = pg.connect(database='citi_bike', user='admiral', password='OpenSesame123', host='localhost', port='5432')

# TODO: Connection Pooling/Retry due to connection issues
def insert_df_to_table(df, table_name):
    df = df.replace({pd.isna: None, np_nan: None})
    values = [tuple(x) for x in df.to_numpy()]
    columns = ', '.join(df.columns)
    sql = f'insert into {table_name} ({columns}) values %s'
    cursor = conn.cursor()
    try:
        pg_extras.execute_values(cursor, sql, values)
        conn.commit()
    except Exception as error:
        print(f'Error: {error}')
        conn.rollback()
        cursor.close()
        return 1
    
def execute(sql, parameters=None):
    cursor = conn.cursor()
    try:
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        conn.commit()
    except Exception as error:
        print(f'Error: {error}')
        conn.rollback()
        cursor.close()
        return 1
