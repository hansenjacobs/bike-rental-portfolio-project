from pandas import DataFrame
from .helpers import df_rename_columns

def process(df: DataFrame):
    col_renames  = {
        'STATION': 'STATION_ID',
        'NAME': 'STATION_NAME',
        'DATE': 'REPORT_DATE',
        'AWND': 'WIND_SPEED_AVERAGE',
        'PGTM': 'PEAK_GUST_TIME',
        'PRCP': 'PRECIPITATION',
        'SNOW': 'SNOW',
        'SNWD': 'SNOW_DEPTH',
        'TAVG': 'TEMPRATURE_AVERAGE',
        'TMAX': 'TEMPRATURE_MAXIMUM',
        'TMIN': 'TEMPRATURE_MINIMUM',
        'TSUN': 'SUN_DURATION',
        'WDF2': 'WIND_DIRECTION_F2',
        'WDF5': 'WIND_DIRECTION_F5',
        'WSF2': 'WIND_SPEED_F2',
        'WSF5': 'WIND_SPEED_F5',
    }
    
    col_add = {
        'WIND_SPEED_UNITS': 'MILES PER HOUR',
        'PRECIPITATION_UNITS': 'INCHES',
        'SNOW_UNITS': 'INCHES',
        'TEMPRATURE_UNITS': 'FAHRENHEIT',
        'SUN_DURATION_UNITS': 'MINUTES',
    }

    col_final_order = [
        'STATION_ID',
        'STATION_NAME',
        'REPORT_DATE',
        'WIND_DIRECTION_F2',
        'WIND_DIRECTION_F5',
        'WIND_SPEED_AVERAGE',
        'WIND_SPEED_F2',
        'WIND_SPEED_F5',
        'WIND_SPEED_UNITS',
        'PEAK_GUST_TIME',
        'PRECIPITATION',
        'PRECIPITATION_UNITS',
        'SNOW',
        'SNOW_DEPTH',
        'SNOW_UNITS',
        'TEMPRATURE_AVERAGE',
        'TEMPRATURE_MAXIMUM',
        'TEMPRATURE_MINIMUM',
        'TEMPRATURE_UNITS',
        'SUN_DURATION',
        'SUN_DURATION_UNITS',
    ]

    df = df_rename_columns(df, col_renames)

    for field, default_value in col_add.items():
        df[field] = default_value

    df = df[col_final_order]

    return df
