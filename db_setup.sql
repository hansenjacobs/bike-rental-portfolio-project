-- Written for PostgreSQL 15.5
-- Create a database on the server and connect to it
-- Once connected to the database of choice, run the following script to create the schema and tables
create schema citi_bike
    authorization admiral;

create table citi_bike.bike_stations(
	id int,
	description varchar(250),
	latitude double precision,
	longitude double precision,
	constraint pk_bike_stations
		primary key (id)
);

create table citi_bike.bike_trips(
	id varchar(100),
	duration int,
	start_datetime timestamp,
	stop_datetime timestamp,
	start_station_id int,
	stop_station_id int,
	bike_id int,
	user_type varchar(25),
	birth_year int,
	gender varchar(1),
	constraint pk_bike_trips
		primary key (id),
	constraint fk_bike_trips_bike_stations_start
		foreign key(start_station_id)
		references citi_bike.bike_stations(id),
	constraint fk_bike_trips_bike_stations_stop
		foreign key(stop_station_id)
		references citi_bike.bike_stations(id)
);

create table citi_bike.weather_stations(
	id varchar(50),
	description varchar(250),
	constraint pk_weather_stations
		primary key(id)
);

create table citi_bike.weather(
	reporting_date date,
	station_id varchar(50),
	wind_direction_f2 int,
	wind_direction_f5 int,
	wind_speed_average real,
	wind_speed_f2 real,
	wind_speed_f5 real,
	wind_speed_unit varchar(25),
	peak_gust_time varchar(25),
	precipitation real,
	precipitation_unit varchar(25),
	snow real,
	snow_depth real,
	snow_unit varchar(25),
	temperature_average int,
	temperature_maximum int,
	temperature_minimum int,
	temperature_unit varchar(25),
	sun_duration int,
	sun_duration_unit varchar(25),
	constraint pk_weather
		primary key(reporting_date, station_id),
	constraint fk_weather_weather_station
		foreign key(station_id)
		references citi_bike.weather_stations(id)
);