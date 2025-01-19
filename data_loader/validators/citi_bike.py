import pandas as pd
import pandera as pa

class Input(pa.DataFrameModel):
    DURATION: int = pa.Field(alias='Trip Duration')
    START_TIME: str = pa.Field(alias='Start Time', nullable=False, str_matches=r'^(\d{4})\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]) ([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$')
    END_TIME: str = pa.Field(alias='Stop Time', nullable=False, str_matches=r'^(\d{4})\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]) ([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$')
    START_STATION_ID: int = pa.Field(alias='Start Station ID', nullable=False)
    START_STATION_NAME: str = pa.Field(alias='Start Station Name')
    START_STATION_LAT: float = pa.Field(alias='Start Station Latitude')
    START_STATION_LON: float = pa.Field(alias='Start Station Longitude')
    END_STATION_ID: int = pa.Field(alias='End Station ID', nullable=False)
    END_STATION_NAME: str = pa.Field(alias='End Station Name')
    END_STATION_LAT: float = pa.Field(alias='End Station Latitude')
    END_STATION_LON: float = pa.Field(alias='End Station Longitude')
    BIKE_ID: int = pa.Field(alias='Bike ID', nullable=False)
    USER_TYPE: str = pa.Field(alias='User Type', isin=['Subscriber','Customer'])
    BIRTH_YEAR: float = pa.Field(alias='Birth Year', nullable=True)
    GENDER: int = pa.Field(alias='Gender', isin=[0, 1, 2])

class Output(pa.DataFrameModel):
    DURATION: int
    START_TIME: pa.DateTime = pa.Field(nullable=False)
    END_TIME: pa.DateTime = pa.Field(nullable=False)
    START_STATION_ID: int = pa.Field(nullable=False)
    START_STATION_NAME: str
    START_STATION_LAT: float
    START_STATION_LON: float
    END_STATION_ID: int = pa.Field(nullable=False)
    END_STATION_NAME: str
    END_STATION_LAT: float
    END_STATION_LON: float
    BIKE_ID: int = pa.Field(nullable=False)
    USER_TYPE: str = pa.Field(isin=['Subscriber','Customer'])
    BIRTH_YEAR: int = pa.Field(nullable=True)
    GENDER: str = pa.Field(isin=['F', 'M', 'U']),
    TRIP_ID: str = pa.Field(nullable=False)

def transform(df):
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
    gender_map = {
        0: 'U',
        1: 'M',
        2: 'F',
    }
    df.rename(columns=col_renames, inplace=True)
    df['START_TIME'] = pd.to_datetime(df['START_TIME'])
    df['END_TIME'] = pd.to_datetime(df['END_TIME'])
    df['BIRTH_YEAR'] = df['BIRTH_YEAR'].astype('Int64') # Use 'Int64' to allow NaN values
    df['GENDER'] = df['GENDER'].map(gender_map)
    df['TRIP_ID'] = df.apply(lambda r: f"{r['BIKE_ID']}_{r['START_STATION_ID']}_{int(r['START_TIME'].timestamp())}")
    return df
