import zipfile
import os
import urllib.request
import pandas as pd
from sqlalchemy import TEXT, FLOAT, INTEGER
import sqlalchemy as sa

def download_gtfs_data(url, zip_file_path):
    urllib.request.urlretrieve(url, zip_file_path)

def read_stops_txt(zip_file_path):
    return pd.read_csv(zipfile.ZipFile(zip_file_path).open('stops.txt'))

def validate_coordinates(lat, lon):
    return -90 <= lat <= 90 and -90 <= lon <= 90

def filter_and_validate_stops(stops_df):
    filtered_stops = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
    filtered_stops = filtered_stops[stops_df['zone_id'] == 2001]
    filtered_stops = filtered_stops[filtered_stops.apply(lambda row: validate_coordinates(row['stop_lat'], row['stop_lon']), axis=1)]
    filtered_stops = filtered_stops.dropna()
    return filtered_stops

def create_sqlite_types():
    return {'stop_id': INTEGER, 'stop_name': TEXT, 'stop_lat': FLOAT, 'stop_lon': FLOAT, 'zone_id': INTEGER}

def write_to_sqlite(data_frame, table_name):
    conn = sa.create_engine("sqlite:///gtfs.sqlite")
    data_frame.to_sql(table_name, conn, index=False, if_exists='replace', dtype=create_sqlite_types())

def main():
    # download gtfs data
    download_gtfs_data("https://gtfs.rhoenenergie-bus.de/GTFS.zip", "GTFS.zip")

    # read only stops.txt directly from the archive
    stops_df = read_stops_txt("GTFS.zip")

    # filter and validate
    filtered_stops = filter_and_validate_stops(stops_df)

    # write data to sqlite
    write_to_sqlite(filtered_stops, 'stops')

    # cleanup
    os.remove('GTFS.zip')

if __name__ == "__main__":
    main()
