import os
import pandas as pd
from .postgres_utils import cleanup_sql, constraints_sql, execute as pg_execute, insert_df_to_table

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
        pg_execute(constraints_sql[data_type]['drop'])
        print('Constraints dropped successfully.')
        for sql, get_values in cleanup_sql[data_type]:
            if get_values:
                values = get_values(df)
                print(f'Cleaning up data with SQL: {sql} and values')
                pg_execute(sql, values)
            else:
                print(f'Cleaning up data with SQL: {sql}')
                pg_execute(sql)

        table_inserts = validator.split_df_by_table(df)
        for table, df in table_inserts.items():
            print(f'Inserting data into table: {table}')
            insert_df_to_table(df, table)

    except Exception as error:
        print(f'ERROR: {error}')
    finally:
        pg_execute(constraints_sql[data_type]['add'])