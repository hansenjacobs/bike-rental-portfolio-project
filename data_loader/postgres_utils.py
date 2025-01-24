import pandas as pd
import psycopg2 as pg
import psycopg2.extras as pg_extras
from numpy import nan as np_nan


def get_db_connection(config=None, host=None, database=None, user=None, password=None, port='5432'):
    try:
        if config:
            host = getattr(config, 'db_host', host)
            database = getattr(config, 'db_name', database)
            user = getattr(config, 'db_user', user)
            password = getattr(config, 'db_password', password)
            port = getattr(config, 'db_port', port)
        conn = pg.connect(database=database, user=user, password=password, host=host, port=port)
        return conn
    except Exception as error:
        print(f'Error connecting to database: {error}')


# TODO: Connection Pooling/Retry due to connection issues
def insert_df_to_table(conn, df, table_name):
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


def execute(conn, sql, parameters=None):
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
