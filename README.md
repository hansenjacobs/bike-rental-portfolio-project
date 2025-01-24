# bike-rental-portfolio-project
A data management project to utilize ETL processing, SQL and database design skills.
This project was adapted from the Data Management Project of the Data Engineer Career Path course at codecadamy.com.

## Project Tasks
 - **Prepare the Data** Review the data to become familiar with it and identify data quality issues.
 - **Create a Schema** Create a schema for the final database. This should include every table, all data types and any constraints - primary keys, foreign keys, ect.
 - **Create the Database** Using the schema create the database and insert the data.
 - **Create Views** Create views in the Postgres database that could be useful for the analytics team.
### Adaption and Implementation of Tasks
I thought the project details lacked an important aspect of Data Engineering, pipelines. It focus more on one time cleansing and database creation. As such, using the project as a jumping off point, I wanted to use it as the bases for creating a process where future data could be ingested and even historical data could be updated if necessary.

As I have more experience with ELT, I decided to go with ETL. Using Pandas Dataframes as the tool to preform the data cleaning/transformation.

### Normalization
The data set has relatively few data points. There are two natural division in the data weather and bike data, I chose to continue with normalization of the weather and bike stations in the respective data sets to include some additional complexity. Additionally, in such a small project/set(s) of data breaking such normalization just didn't seem to make sense.

## How does it work?
The intent of the project is to execute the module with arguments of data type (trip or weather) and file path.

Using this information, the project will:
1. Check the file exists
1. Validate the input matches the schema
1. Transform the data
1. Validate the transformed data
1. Delete data from the database that is being imported again (to allow for updates to existing data)
1. Insert the data

## To Do
The current project works, but there would be changes/enhancements that I would like to implement:
1. Create unit and integration/e2e testing
1. Move all of the SQL to the postgres utils. Keep only the most basic of information needed in the validators, such as, table names, constraint names, primary key fields
1. Adjust functions and calls to always delete the incoming pk's from the table before inserting
1. Better incoming data validation
    a. Ensure each date only appears once in the weather file
    a. Ensure a bike trip only appears once in every file
1. Implement a makefile for easier setup

## Setup
### Database
Created project using Postgres v15.5
1. Install and setup postgres (docker recommended)
1. Create a database
1. Create `citi_bike` schema within the database
1. Execute the sql from `db_setup.sql` (provided in the project root)
### Python
Created project using 3.12
1. Create virtual environment using your preferred method
1. Activate your virtual environment
1. `pip install -r requirements.txt`
1. `cp .env_example .env`
1. Edit the `.env` file with your Postgres details
1. `python -m data_loader -t weather -f bike-rental-starter-kit/data/newark_airport_2016.csv`
1. `python -m data_loader -t trip -f bike-rental-starter-kit/data/JC-201601-citibike-tripdata.csv`
