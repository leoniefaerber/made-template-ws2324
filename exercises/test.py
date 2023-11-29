import sqlalchemy as sa
import pandas as pd

# can be used to to extract a csv file from an url and load the data into a sqlite database

def automated_pipeline():

    # files
    source_url = ""
    db_file = 'test.sqlite'

    # extract data from source (delimiter = ';')
    data = pd.read_csv(source_url, ';')

    # sqlite engine (three slashes for relative path)
    engine = sa.create_engine(f'sqlite:///{db_file}', echo=False)

    # load to file (replaces file content if already exists; no index; uses declared dtype)
    data.to_sql("test", con=engine, if_exists='replace', index=False)
   

if __name__ == '__main__':
    automated_pipeline()