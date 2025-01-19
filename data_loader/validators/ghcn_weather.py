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
    STATION_ID: str = pa.Field(nullable=False)
    STATION_NAME: str = pa.Field(nullable=True)
    REPORT_DATE: pa.DateTime = pa.Field(nullable=False)
    WIND_DIRECTION_F2: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 360})
    WIND_DIRECTION_F5: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 360})
    WIND_SPEED_AVERAGE: float = pa.Field(nullable=True, ge=0)
    WIND_SPEED_F2: float = pa.Field(nullable=True, ge=0)
    WIND_SPEED_F5: float = pa.Field(nullable=True, ge=0)
    WIND_SPEED_UNITS: str
    PEAK_GUST_TIME: str = pa.Field(nullable=True)
    PRECIPITATION: float = pa.Field(ge=0)
    PRECIPITATION_UNITS: str
    SNOW: float = pa.Field(ge=0)
    SNOW_DEPTH: float = pa.Field(nullable=True, ge=0)
    SNOW_UNITS: str
    TEMPRATURE_AVERAGE: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    TEMPRATURE_MAXIMUM: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    TEMPRATURE_MINIMUM: int = pa.Field(in_range={"min_value": -150, "max_value": 150})
    TEMPRATURE_UNITS: str
    SUN_DURATION: int = pa.Field(nullable=True, in_range={"min_value": 0, "max_value": 1440})
    SUN_DURATION_UNITS: str

def transform(df):
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
    df.rename(columns=col_renames, inplace=True)
    df['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'])
    df['WIND_SPEED_UNITS'] = 'MILES PER HOUR'
    df['WIND_DIRECTION_F2'] = df['WIND_DIRECTION_F2'].astype('Int64')
    df['WIND_DIRECTION_F5'] = df['WIND_DIRECTION_F5'].astype('Int64')
    df['PRECIPITATION_UNITS'] = 'INCHES'
    df['SNOW_UNITS'] = 'INCHES'
    df['TEMPRATURE_UNITS'] = 'FAHRENHEIT'
    df['TEMPRATURE_AVERAGE'] = df['TEMPRATURE_AVERAGE'].astype('Int64')
    df['TEMPRATURE_MAXIMUM'] = df['TEMPRATURE_MAXIMUM'].astype('Int64')
    df['TEMPRATURE_MINIMUM'] = df['TEMPRATURE_MINIMUM'].astype('Int64')
    df['SUN_DURATION_UNITS'] = 'MINUTES'
    df['SUN_DURATION'] = df['SUN_DURATION'].astype('Int64')