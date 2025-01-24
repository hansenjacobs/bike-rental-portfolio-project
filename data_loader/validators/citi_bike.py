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
    USER_TYPE: str = pa.Field(alias='User Type', isin=['Subscriber', 'Customer'], nullable=True)
    BIRTH_YEAR: float = pa.Field(alias='Birth Year', nullable=True)
    GENDER: int = pa.Field(alias='Gender', isin=[0, 1, 2])


class Output(pa.DataFrameModel):
    duration: int
    start_datetime: pa.DateTime = pa.Field(nullable=False)
    stop_datetime: pa.DateTime = pa.Field(nullable=False)
    start_station_id: int = pa.Field(nullable=False)
    start_station_name: str
    start_station_latitude: float
    start_station_longitude: float
    stop_station_id: int = pa.Field(nullable=False)
    stop_station_name: str
    stop_station_latitude: float
    stop_station_longitude: float
    bike_id: int = pa.Field(nullable=False)
    user_type: str = pa.Field(isin=['Subscriber', 'Customer'], nullable=True)
    birth_year: int = pa.Field(nullable=True)
    gender: str = pa.Field(isin=['F', 'M', 'U'])
    id: str = pa.Field(nullable=False)


def _get_station_df(df):
    station_df = pd.concat(
        [
            df[['start_station_id', 'start_station_name', 'start_station_latitude', 'start_station_longitude']].rename(
                columns={
                    'start_station_id': 'id',
                    'start_station_name': 'description',
                    'start_station_latitude': 'latitude',
                    'start_station_longitude': 'longitude',
                }
            ),
            df[['stop_station_id', 'stop_station_name', 'stop_station_latitude', 'stop_station_longitude']].rename(
                columns={
                    'stop_station_id': 'id',
                    'stop_station_name': 'description',
                    'stop_station_latitude': 'latitude',
                    'stop_station_longitude': 'longitude',
                }
            )
        ]
    )
    return station_df.drop_duplicates(subset='id').reset_index(drop=True)


def split_df_by_table(df):
    retval = {}
    bike_trips_table_fields = [
        'duration',
        'start_datetime',
        'stop_datetime',
        'start_station_id',
        'stop_station_id',
        'bike_id',
        'user_type',
        'birth_year',
        'gender',
        'id'
    ]
    table_fields = [
        ('citi_bike.bike_stations', lambda df: _get_station_df(df)),
        ('citi_bike.bike_trips', lambda df: df[bike_trips_table_fields])
    ]
    for table, get_df in table_fields:
        retval[table] = get_df(df)
    return retval


def transform(df):
    col_renames = {
        'Trip Duration': 'duration',
        'Start Time': 'start_datetime',
        'Stop Time': 'stop_datetime',
        'Start Station ID': 'start_station_id',
        'Start Station Name': 'start_station_name',
        'Start Station Latitude': 'start_station_latitude',
        'Start Station Longitude': 'start_station_longitude',
        'End Station ID': 'stop_station_id',
        'End Station Name': 'stop_station_name',
        'End Station Latitude': 'stop_station_latitude',
        'End Station Longitude': 'stop_station_longitude',
        'Bike ID': 'bike_id',
        'User Type': 'user_type',
        'Birth Year': 'birth_year',
        'Gender': 'gender',
    }
    gender_map = {
        0: 'U',
        1: 'M',
        2: 'F',
    }
    df.rename(columns=col_renames, inplace=True)
    df['start_datetime'] = pd.to_datetime(df['start_datetime'])
    df['stop_datetime'] = pd.to_datetime(df['stop_datetime'])
    df['birth_year'] = df['birth_year'].astype('Int64')  # Use 'Int64' to allow NaN values
    df['gender'] = df['gender'].map(gender_map)
    df['id'] = df.apply(lambda r: f"{r['bike_id']}_{r['start_station_id']}_{int(r['start_datetime'].timestamp())}", axis=1)
    return df


constraints_sql = {
    'drop': ["alter table citi_bike.bike_trips drop constraint fk_bike_trips_bike_stations_start, drop constraint fk_bike_trips_bike_stations_stop",],
    'add': ["alter table citi_bike.bike_trips add constraint fk_bike_trips_bike_stations_start foreign key(start_station_id) references citi_bike.bike_stations(id), add constraint fk_bike_trips_bike_stations_stop foreign key(stop_station_id) references citi_bike.bike_stations(id)",],
}
cleanup_sql = {
    'citi_bike.bike_trips': ("delete from citi_bike.bike_trips where id in %(id)s", lambda df: {'id': tuple(set((df['id'].to_list())))}),
    'citi_bike.bike_stations': ("delete from citi_bike.bike_stations where id in %(id)s", lambda df: {'id': tuple(set(df['id'].unique().tolist()))}),
}
