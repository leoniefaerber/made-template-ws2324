# automated data pipeline that
# - pulls the project data sets from the internet,
# - transforms it and fixes errors,
# - stores the data in the /data directory

import sqlalchemy as sa
from sqlalchemy.types import BIGINT, TEXT, FLOAT
import pandas as pd

def load_to_sqlite_file(data, db_name, dtype, engine):

    # load to file (replaces file content if already exists; no index; uses declared dtype)
    data.to_sql(db_name, con=engine, if_exists='replace', index=False, dtype=dtype)


# performs transformations that can be applied to all data sources
def perform_basic_data_cleaning(df):
    df = df.copy()

    # drop null values in the country and the value column
    df = df.dropna(subset=['geo', 'OBS_VALUE'])

    # check standard values
    freq_value = "Annual"
    unit_value = "Percentage"
    isced11_value = "All ISCED 2011 levels"
    sex_value = "Total"
    year1_value = 2014
    year2_value = 2019
    condition = (
        (df['freq'] == freq_value) &
        (df['unit'] == unit_value) &
        (df['isced11'] == isced11_value) &
        (df['sex'] == sex_value) &
        ((df['TIME_PERIOD'] == year1_value) | (df['TIME_PERIOD'] == year2_value))
    )
    df = df[condition]

    # drop generalized European information
    geo_excluded = "Euro"
    df = df[~df["geo"].str.contains(geo_excluded)]

    # the observation flag has to be null to exclude unusable data points
    df = df[df['OBS_FLAG'].isnull()]

    return df


# drops columns that can be dropped from all the source tables
def drop_columns(df):
    return df.drop(columns=['DATAFLOW', 'LAST UPDATE', 'freq', 'unit', 'isced11', 'sex', 'age', 'OBS_FLAG'])

     

def create_activity_table(activity_data, db_name, engine):

    # clean
    activity_data = perform_basic_data_cleaning(activity_data)

    # drop rows with null values in duration column
    activity_data = activity_data.dropna(subset=['duration'])

    # check for activity specific values
    age_value = "Total"
    duration_excluded_value = "150 minutes or over"
    activity_data = activity_data.loc[activity_data["age"] == age_value]
    activity_data = activity_data.loc[activity_data["duration"] != duration_excluded_value]

    # map duration strings to floats
    duration_value_mapping = {
        'Zero minutes': 0, 
        'From 1 to 149 minutes': 1 + ((149 - 1)/2), 
        'From 150 to 299 minutes': 150 + ((299-150)/2),
        '300 minutes or over': 300
    }
    activity_data.loc[:, 'duration'] = activity_data['duration'].replace(duration_value_mapping)
    
    # drop unnecessary columns
    activity_data = drop_columns(activity_data)

    # rename columns for readability
    activity_column_names = {
        'TIME_PERIOD': 'year',
        'OBS_VALUE': 'percentage_of_population',
        'duration': 'duration_in_min',
        'geo': 'country'
    }
    activity_data = activity_data.rename(columns=activity_column_names)

    # load
    activity_dtype = {
        'duration_in_min': FLOAT,
        'country': TEXT,
        'year': BIGINT,
        'percentage_of_population': FLOAT,
    }
    load_to_sqlite_file(activity_data, db_name, activity_dtype, engine)



def create_mental_health_table(mental_health_data, db_name, engine):

    # clean
    mental_health_data = perform_basic_data_cleaning(mental_health_data)
    
    # drop rows with null values in the hlth_pb column
    mental_health_data = mental_health_data.dropna(subset=['hlth_pb'])
    
    # check for mental health specific values
    age_value = "Total"
    hlth_pb_excluded = "Depressive symptoms"
    condition = (mental_health_data['age'] == age_value) & (mental_health_data['hlth_pb'] != hlth_pb_excluded)
    mental_health_data = mental_health_data[condition]

    # map symptom strings to ints
    hlth_pb_value_mapping = {
        'Other depressive symptoms': 1, 
        'Major depressive symptoms': 2, 
    }
    mental_health_data.loc[:, 'hlth_pb'] = mental_health_data['hlth_pb'].replace(hlth_pb_value_mapping)
    
    # drop unnecessary columns
    mental_health_data = drop_columns(mental_health_data)
    
    # rename columns for readability
    mental_health_column_names = {
        'TIME_PERIOD': 'year',
        'OBS_VALUE': 'percentage_of_population',
        'hlth_pb': 'depressive_symptoms',
        'geo': 'country'
    }
    mental_health_data = mental_health_data.rename(columns=mental_health_column_names)

    # load
    mental_health_dtype = {
        'depressive_symptoms': BIGINT,
        'country': TEXT,
        'year': BIGINT,
        'percentage_of_population': FLOAT,
    }
    load_to_sqlite_file(mental_health_data, db_name, mental_health_dtype, engine)


def create_general_health_table(general_health_data, db_name, engine):

    # clean
    general_health_data = perform_basic_data_cleaning(general_health_data)
    
    # drop rows with null values in the levels column
    general_health_data = general_health_data.dropna(subset=['levels'])
    
    # check for general health specific values
    age_value = "16 years or over"
    levels_excluded = "or"
    condition = (general_health_data['age'] == age_value) & (~general_health_data["levels"].str.contains(levels_excluded))
    general_health_data = general_health_data[condition]
    
    # map health strings to ints
    general_health_value_mapping = {
        'Very good': 2, 
        'Good': 1, 
        'Fair': 0,
        'Bad': -1,
        'Very bad': -2,
    }
    general_health_data.loc[:, 'levels'] = general_health_data['levels'].replace(general_health_value_mapping)
    
    # drop unnecessary columns
    general_health_data = drop_columns(general_health_data)
    
    # rename columns for readability
    general_health_column_names = {
        'TIME_PERIOD': 'year',
        'OBS_VALUE': 'percentage_of_population',
        'levels': 'general_health',
        'geo': 'country'
    }
    general_health_data = general_health_data.rename(columns=general_health_column_names)

    # load
    general_health_dtype = {
        'general_health': BIGINT,
        'country': TEXT,
        'year': BIGINT,
        'percentage_of_population': FLOAT
    }
    load_to_sqlite_file(general_health_data, db_name, general_health_dtype, engine)


def create_all_tables():

    # sqlite engine (three slashes for relative path)
    engine = sa.create_engine(f'sqlite:///data/result.sqlite')

    activty_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/HLTH_EHIS_PE2E/?format=SDMX-CSV&lang=en&label=label_only'
    activity_data = pd.read_csv(activty_source_url)
    create_activity_table(activity_data, 'activity', engine)


    mental_health_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_ehis_mh1e/?format=SDMX-CSV&lang=en&label=label_only'
    mental_health_data = pd.read_csv(mental_health_source_url)
    create_mental_health_table(mental_health_data, 'mental_health', engine)

    general_health_source_url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_silc_02/?format=SDMX-CSV&lang=en&label=label_only'
    general_health_data = pd.read_csv(general_health_source_url)
    create_general_health_table(general_health_data, 'general_health', engine)


if __name__ == '__main__':
    create_all_tables()

