from pandas import DataFrame

def df_rename_columns(df: DataFrame, map: dict):
    if df.columns.to_list() != list(map.keys()):
        error_message = f'''
        Invalid file: header does not match expected columns.
        Expected header: {', '.join(list(map.keys()))}
        Actual header: {', '.join(df.columns.to_list())}
        '''.strip()
        raise ValueError(error_message)

    df.rename(columns=map, inplace=True)
    return df