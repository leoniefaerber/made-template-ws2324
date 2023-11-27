import sqlalchemy as sa
from sqlalchemy.types import BIGINT, TEXT, FLOAT
import pandas as pd


def automated_pipeline():

    # files
    source_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    db_file = 'test.sqlite'

    # extract data from source (delimiter = ';')
    data = pd.read_csv(source_url, ';')

    # sqlite engine (three slashes for relative path)
    engine = sa.create_engine(f'sqlite:///{db_file}', echo=False)

    # load to file (replaces file content if already exists; no index; uses declared dtype)
    data.to_sql("test", con=engine, if_exists='replace', index=False)
   

if __name__ == '__main__':
    automated_pipeline()