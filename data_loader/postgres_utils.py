import pandas as pd
import psycopg2 as pg
import psycopg2.extras as pg_extras

conn = pg.connect(database='citi_bike', user='admiral', password='OpenSesame123', host='localhost', port='5432')

# TODO: Connection Pooling/Retry due to connection issues
def insert_df_to_table(df, table_name):
    df = df.replace({pd.NA: None})
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
        return 1
    
def execute(sql, parameters=None):
    cursor = conn.cursor()
    try:
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
    except Exception as error:
        print(f'Error: {error}')
        conn.rollback()
        cursor.close()
        return 1

# TODO: Either update existing data if part of FK or find better solution than dropping and re-creating constraints
constraints_sql = {
    'trips': {
        'drop': "alter table citi_bike.bike_trips drop constraint fk_bike_trips_bike_stations_start, drop constraint fk_bike_trips_bike_stations_stop",
        'add': "alter table citi_bike.bike_trips add constraint fk_bike_trips_bike_stations_start foreign key(start_station_id) references citi_bike.bike_stations(id), add constraint fk_bike_trips_bike_stations_stop foreign key(start_station_id) references citi_bike.bike_stations(id)"
    },
    'weather': {
        'drop': "alter table citi_bike.weather drop constraint fk_weather_weather_station",
        'add': "alter table citi_bike.weather add constraint fk_weather_weather_station foreign key(station_id) references citi_bike.weather_stations(id)"
    }
}
cleanup_sql = {
    'trips': [
        ("delete from citi_bike.bike_trips where id in (%(id)s)", lambda df: {'id': "'" + "', '".join(df['id'].to_list()) + "'"}),
        ("delete from citi_bike.bike_stations where id in %(id)s", lambda df: {'id': tuple(set(df['start_station_id'].unique().tolist() + 
                                                                                      df['stop_station_id'].unique().tolist()))}),
    ],
    'weather': [
        ("delete from citi_bike.weather where (reporting_date, station_id) in (%(id)s)", 
            lambda df: {
                'id': list(zip(df['reporting_date'].dt.strftime('%Y-%m-%d'), df['station_id']))
            }),
        ("delete from citi_bike.weather_stations where id in (%(id)s)", lambda df: {'id': df['station_id'].unique().tolist()}),
    ]
}