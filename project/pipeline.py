# automated data pipeline that
# - pulls the project data sets from the internet,
# - transforms it and fixes errors,
# - stores the data in the /data directory

import sqlalchemy as sa
from sqlalchemy.types import BIGINT, TEXT, FLOAT
import pandas as pd

def load_to_sqlite_file(data, db_file, db_name, dtype):

    # sqlite engine (three slashes for relative path)
    engine = sa.create_engine(f'sqlite:///data/{db_file}')

    # load to file (replaces file content if already exists; no index; uses declared dtype)
    data.to_sql(db_name, con=engine, if_exists='replace', index=False, dtype=dtype)
     

def create_activity_table(activity_data, db_file, db_name):

    # clean
    # drop not needed columns
    activity_data = activity_data.drop(columns=['DATAFLOW', 'LAST UPDATE'])
    # drop rows with null values in indispensable columns
    activity_data = activity_data.dropna(subset=['duration', 'geo', 'TIME_PERIOD', 'OBS_VALUE'])
    # rename columns for uniform naming
    activity_column_names = {
        'freq': 'frequency',
        'TIME_PERIOD': 'time_period',
        'OBS_VALUE': 'obs_value',
        'OBS_FLAG': 'obs_flag'
    }
    activity_data = activity_data.rename(columns=activity_column_names)

    # load
    activity_dtype = {
        'frequency': TEXT,
        'unit': TEXT,
        'duration': TEXT,
        'isced11': TEXT,
        'sex': TEXT,
        'age': TEXT,
        'geo': TEXT,
        'time_period': BIGINT,
        'obs_value': FLOAT,
        'obs_flag': TEXT
    }
    load_to_sqlite_file(activity_data, db_file, db_name, activity_dtype)


def create_mental_health_table(mental_health_data, db_file, db_name):

    # clean
    # drop not needed columns
    mental_health_data = mental_health_data.drop(columns=['DATAFLOW', 'LAST UPDATE'])
    # drop rows with null values in indispensable columns
    mental_health_data = mental_health_data.dropna(subset=['hlth_pb', 'geo', 'TIME_PERIOD', 'OBS_VALUE'])
    # rename columns for uniform naming
    mental_health_column_names = {
        'freq': 'frequency',
        'TIME_PERIOD': 'time_period',
        'OBS_VALUE': 'obs_value',
        'OBS_FLAG': 'obs_flag'
    }
    mental_health_data = mental_health_data.rename(columns=mental_health_column_names)

    # load
    mental_health_dtype = {
        'frequency': TEXT,
        'unit': TEXT,
        'isced11': TEXT,
        'hlth_pb': TEXT,
        'sex': TEXT,
        'age': TEXT,
        'geo': TEXT,
        'time_period': BIGINT,
        'obs_value': FLOAT,
        'obs_flag': TEXT
    }
    load_to_sqlite_file(mental_health_data, db_file, db_name, mental_health_dtype)


def create_general_health_table(general_health_data, db_file, db_name):

    # clean
    # drop not needed columns
    general_health_data = general_health_data.drop(columns=['DATAFLOW', 'LAST UPDATE'])
    # drop rows with null values in indispensable columns
    general_health_data = general_health_data.dropna(subset=['levels', 'geo', 'TIME_PERIOD', 'OBS_VALUE'])
    # rename columns for uniform naming
    general_health_column_names = {
        'freq': 'frequency',
        'TIME_PERIOD': 'time_period',
        'OBS_VALUE': 'obs_value',
        'OBS_FLAG': 'obs_flag'
    }
    general_health_data = general_health_data.rename(columns=general_health_column_names)

    #load
    general_health_dtype = {
        'frequency': TEXT,
        'unit': TEXT,
        'isced11': TEXT,
        'age': TEXT,
        'sex': TEXT,
        'levels': TEXT,
        'geo': TEXT,
        'time_period': BIGINT,
        'obs_value': FLOAT,
        'obs_flag': TEXT
    }
    load_to_sqlite_file(general_health_data, db_file, db_name, general_health_dtype)


def create_all_tables():

    activtiy_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/HLTH_EHIS_PE2E/?format=SDMX-CSV&lang=de&label=label_only'
    activity_data = pd.read_csv(activtiy_source_url)
    create_activity_table(activity_data, 'activity.sqlite', 'activity')


    mental_health_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_ehis_mh1e/?format=SDMX-CSV&lang=de&label=label_only'
    mental_health_data = pd.read_csv(mental_health_source_url)
    create_mental_health_table(mental_health_data, 'mental_health.sqlite', 'mental_health')

    general_health_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_silc_02/?format=SDMX-CSV&lang=de&label=label_only'
    general_health_data = pd.read_csv(general_health_source_url)
    create_general_health_table(general_health_data, 'general_health.sqlite', 'general_health')


if __name__ == '__main__':
    create_all_tables()

