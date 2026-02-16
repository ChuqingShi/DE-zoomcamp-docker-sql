#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64", # nullable integer
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run(pg_user = 'root', 
        pg_password = 'root', 
        pg_host = 'localhost', 
        pg_port = 5432, 
        pg_db = 'ny_taxi', 
        year = 2021, 
        month = 1, 
        target_table = "yellow_taxi_data", 
        chunksize = 100000):


    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    
    engine = create_engine(f"postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name= target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name= target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))


@click.command()
@click.option("--pg-user", default="root", help="Postgres user")
@click.option("--pg-password", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5432, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database name")
@click.option("--year", default=2021, type=int, help="Year of the dataset")
@click.option("--month", default=1, type=int, help="Month of the dataset")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("--chunksize", default=100000, type=int, help="CSV read chunksize")
def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """CLI wrapper for `run`.

    Defaults mirror the original function defaults.
    """
    run(
        pg_user=pg_user,
        pg_password=pg_password,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    main()


