import pandas as pd
import pandera as pa

class Input(pa.DataFrameModel):
    STATION: str = pa.Field(nullable=False)
    NAME: str = pa.Field(nullable=True)
    DATE: str = pa.Field(nullable=False, str_matches=r'^(\d{4})\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$')
    AWND: float = pa.Field(coerce=True)
    PGTM: str = pa.Field(nullable=True)
    PRCP: float = pa.Field(coerce=True)
    SNOW: float = pa.Field(coerce=True)
    SNWD: float = pa.Field(nullable=True, coerce=True)
    TAVG: float = pa.Field(coerce=True)
    TMAX: float = pa.Field(coerce=True)
    TMIN: float = pa.Field(coerce=True)
    TSUN: float = pa.Field(nullable=True, coerce=True)
    WDF2: float = pa.Field(nullable=True, coerce=True)
    WDF5: float = pa.Field(nullable=True, coerce=True)
    WSF2: float = pa.Field(nullable=True)
    WSF5: float = pa.Field(nullable=True)

class Output(pa.DataFrameModel):
    station_id: str = pa.Field(nullable=False)
    station_name: str = pa.Field(nullable=True)
    reporting_date: pa.DateTime = pa.Field(nullable=False)
    wind_direction_f2: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 360})
    wind_direction_f5: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 360})
    wind_speed_average: float = pa.Field(nullable=True, ge=0)
    wind_speed_f2: float = pa.Field(nullable=True, ge=0)
    wind_speed_f5: float = pa.Field(nullable=True, ge=0)
    wind_speed_unit: str
    peak_gust_time: str = pa.Field(nullable=True)
    precipitation: float = pa.Field(ge=0)
    precipitation_unit: str
    snow: float = pa.Field(ge=0)
    snow_depth: float = pa.Field(nullable=True, ge=0)
    snow_unit: str
    temperature_average: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    temperature_maximum: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    temperature_minimum: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    temperature_unit: str
    sun_duration: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 1440})
    sun_duration_unit: str


def _get_station_df(df):
    station_df = df[['station_id', 'station_name']].rename(
        columns={
            'station_id': 'id',
            'station_name': 'description',
        }
    )
    return station_df.drop_duplicates(subset='id').reset_index(drop=True)


def split_df_by_table(df):
    retval = {}
    weather_table_fields = [
        'station_id',
        'reporting_date',
        'wind_direction_f2',
        'wind_direction_f5',
        'wind_speed_average',
        'wind_speed_f2',
        'wind_speed_f5',
        'wind_speed_unit',
        'peak_gust_time',
        'precipitation',
        'precipitation_unit',
        'snow',
        'snow_depth',
        'snow_unit',
        'temperature_average',
        'temperature_maximum',
        'temperature_minimum',
        'temperature_unit',
        'sun_duration',
        'sun_duration_unit'
    ]
    table_fields = [
        ('citi_bike.weather_stations', lambda df: _get_station_df(df)),
        ('citi_bike.weather', lambda df: df[weather_table_fields])
    ]
    for table, get_df in table_fields:
        retval[table] = get_df(df)
    
    return retval


def transform(df):
    col_renames  = {
        'STATION': 'station_id',
        'NAME': 'station_name',
        'DATE': 'reporting_date',
        'AWND': 'wind_speed_average',
        'PGTM': 'peak_gust_time',
        'PRCP': 'precipitation',
        'SNOW': 'snow',
        'SNWD': 'snow_depth',
        'TAVG': 'temperature_average',
        'TMAX': 'temperature_maximum',
        'TMIN': 'temperature_minimum',
        'TSUN': 'sun_duration',
        'WDF2': 'wind_direction_f2',
        'WDF5': 'wind_direction_f5',
        'WSF2': 'wind_speed_f2',
        'WSF5': 'wind_speed_f5',
    }
    df.rename(columns=col_renames, inplace=True)
    df['reporting_date'] = pd.to_datetime(df['reporting_date'])
    df['wind_speed_unit'] = 'MILES PER HOUR'
    df['wind_direction_f2'] = df['wind_direction_f2'].astype('Int64')
    df['wind_direction_f5'] = df['wind_direction_f5'].astype('Int64')
    df['precipitation_unit'] = 'INCHES'
    df['snow_unit'] = 'INCHES'
    df['temperature_unit'] = 'FAHRENHEIT'
    df['temperature_average'] = df['temperature_average'].astype('Int64')
    df['temperature_maximum'] = df['temperature_maximum'].astype('Int64')
    df['temperature_minimum'] = df['temperature_minimum'].astype('Int64')
    df['sun_duration_unit'] = 'MINUTES'
    df['sun_duration'] = df['sun_duration'].astype('Int64')
    return df

constraints_sql = {
    'drop': ["alter table citi_bike.weather drop constraint fk_weather_weather_station",],
    'add': ["alter table citi_bike.weather add constraint fk_weather_weather_station foreign key(station_id) references citi_bike.weather_stations(id)",],
}
cleanup_sql = {
    'citi_bike.weather': ("delete from citi_bike.weather where (reporting_date, station_id) in %(id)s", 
            lambda df: {
                'id': tuple(set(list(zip(df['reporting_date'].dt.strftime('%Y-%m-%d'), df['station_id']))))
            }),
    'citi_bike.weather_stations': ("delete from citi_bike.weather_stations where id in %(id)s", lambda df: {'id': tuple(set(df['id'].unique().tolist()))}),
}