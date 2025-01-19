from pandas import DataFrame
from .helpers import df_rename_columns

def process(df: DataFrame):
    col_renames = {
        	'Trip Duration': 'DURATION',
            'Start Time': 'START_TIME',
            'Stop Time': 'END_TIME',
            'Start Station ID': 'START_STATION_ID',
            'Start Station Name': 'START_STATION_NAME',
            'Start Station Latitude': 'START_STATION_LAT',
            'Start Station Longitude': 'START_STATION_LON',
            'End Station ID': 'END_STATION_ID',
            'End Station Name': 'END_STATION_NAME',
            'End Station Latitude': 'END_STATION_LAT',
            'End Station Longitude': 'END_STATION_LON',
            'Bike ID': 'BIKE_ID',
            'User Type': 'USER_TYPE',
            'Birth Year': 'BIRTH_YEAR',
            'Gender': 'GENDER',
    }

    df = df_rename_columns(df, col_renames)

    return df