#!/usr/bin/env python

import argparse
import pandas as pd
from sqlalchemy import create_engine


def ingest_data(params):
    """Ingest data into postgres database

    Args:
        params (dict): Dictionary with database connection parameters
    """
    db = params.db
    user = params.user
    host = params.host
    port = params.port
    password = params.password
    db_table = params.db_table
    parquet_url = params.parquet_url

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(parquet_url)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    schema = pd.io.sql.get_schema(df, name=db_table, con=engine)

    print('Database table schema\n', schema)

    # create table
    df.head(0).to_sql(name=db_table, con=engine,
                      if_exists='replace', index=False)

    # how to insert data in chunks
    # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table

    chunk = 100000

    for i in range(0, df.shape[0], chunk):
        df.iloc[i:i+chunk]\
            .to_sql(name=db_table, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Ingest data into postgres')

    parser.add_argument('--user', type=str, required=True,
                        help='Database user')
    parser.add_argument('--password', type=str,
                        required=True, help='Database password')
    parser.add_argument('--host', type=str,
                        help='Database host', default='localhost')
    parser.add_argument('--port', type=int, help='Database port', default=5432)
    parser.add_argument('--db', type=str, required=True, help='Database name')
    parser.add_argument('--db_table', type=str,
                        required=True, help='Database table name')
    parser.add_argument('--parquet_url', required=True, type=str,
                        help='Parquet file url. It can be a local file or a URL')

    args = parser.parse_args()

    print('Ingesting data...')

    try:
        ingest_data(args)

        print('Data ingested successfully')
    except Exception as e:
        print(f'Error ingesting data: {e}')
