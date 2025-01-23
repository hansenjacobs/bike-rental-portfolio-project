import os
import pandas as pd
from .postgres_utils import execute as pg_execute, insert_df_to_table

def main(filename: str, data_type: str):
    if not os.path.exists(filename):
        print(f'File {filename} does not exist.')
        return 1
    
    # TODO: Update import to be more dynamic, possible options: __import__, imp, or importlib
    match data_type:
        case 'trips':
            from .validators import citi_bike as validator
        case 'weather':
            from .validators import ghcn_weather as validator

    df = pd.read_csv(filename)

    validator.Input.validate(df, lazy=True)
    df = validator.transform(df)
    validator.Output.validate(df, lazy=True)

    print('Data validated successfully.')
    
    try:
        for sql in validator.constraints_sql['drop']:
            pg_execute(sql)
            print(f'Constraints dropped successfully: {sql}')

        table_inserts = validator.split_df_by_table(df)

        for table, df in table_inserts.items():
            print(f'Deleting from {table}')
            delete_sql, get_df = validator.cleanup_sql[table]
            pg_execute(delete_sql, get_df(df))
            print(f'Inserting data into table: {table}')
            insert_df_to_table(df, table)
            print(f'Updated {table} successfully.')

    except Exception as error:
        print(f'ERROR: {error}')
    finally:
        for sql in validator.constraints_sql['add']:
            pg_execute(sql)
            print(f'Constraints added successfully: {sql}')