import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    parquet_name = 'green_tripdata_2025-11.parquet'
    zones_csv = 'taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Load Taxi Data
    print(f"Loading {parquet_name}...")
    df = pd.read_parquet(parquet_name)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("Taxi data loaded successfully!")

    # Load Zones Data
    print(f"Loading {zones_csv}...")
    df_zones = pd.read_csv(zones_csv)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)
    print("Zones data loaded successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    parser.add_argument('--user', help='user name for postgres', default='postgres')
    parser.add_argument('--password', help='password for postgres', default='postgres')
    parser.add_argument('--host', help='host for postgres', default='localhost')
    parser.add_argument('--port', help='port for postgres', default='5432')
    parser.add_argument('--db', help='database name for postgres', default='ny_taxi')
    parser.add_argument('--table_name', help='name of the table where we will write the results to', default='green_tripdata')

    args = parser.parse_args()
    main(args)